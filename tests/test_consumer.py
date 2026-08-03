from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

import app.workers.consumer as consumer_module
from app.core.exceptions import (
    InvalidPaymentStateError,
    WebhookDeliveryError,
    WebhookDeliveryInProgressError,
)
from app.domain.enums import PaymentStatus


class MessageStub:
    def __init__(
        self,
        *,
        retry_count: object = 0,
        message_id: str | None = "event-1",
    ) -> None:
        self.headers = {consumer_module.RETRY_HEADER: retry_count}
        self.message_id = message_id
        self.ack = AsyncMock()
        self.nack = AsyncMock()
        self.reject = AsyncMock()


def make_logger() -> Mock:
    return Mock()


async def test_consumer_acknowledges_successful_processing(
    monkeypatch,
) -> None:
    payment_id = uuid4()
    payment = SimpleNamespace(
        payment_id=payment_id,
        status=PaymentStatus.SUCCEEDED,
    )
    process = AsyncMock(return_value=payment)
    monkeypatch.setattr(consumer_module.processor, "process", process)
    message = MessageStub()
    logger = make_logger()

    await consumer_module.handle_payment_message(
        {"payment_id": str(payment_id)},
        message,  # type: ignore[arg-type]
        logger,
    )

    process.assert_awaited_once()
    assert process.await_args.args[0].payment_id == payment_id
    message.ack.assert_awaited_once_with()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()
    logger.info.assert_called_once()


async def test_consumer_republishes_first_failed_attempt(
    monkeypatch,
) -> None:
    payment_id = uuid4()
    monkeypatch.setattr(
        consumer_module.processor,
        "process",
        AsyncMock(side_effect=WebhookDeliveryError("unavailable")),
    )
    sleep = AsyncMock()
    publish = AsyncMock()
    monkeypatch.setattr(consumer_module.asyncio, "sleep", sleep)
    monkeypatch.setattr(consumer_module.broker, "publish", publish)
    message = MessageStub()
    logger = make_logger()
    payload = {"payment_id": str(payment_id)}

    await consumer_module.handle_payment_message(
        payload,
        message,  # type: ignore[arg-type]
        logger,
    )

    sleep.assert_awaited_once_with(consumer_module.settings.consumer_retry_base_seconds)
    publish.assert_awaited_once_with(
        payload,
        queue=consumer_module.payment_queue,
        exchange=consumer_module.payment_exchange,
        persist=True,
        mandatory=True,
        message_id="event-1",
        headers={consumer_module.RETRY_HEADER: 1},
    )
    message.ack.assert_awaited_once_with()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()


async def test_consumer_rejects_third_failure_to_dlq(monkeypatch) -> None:
    monkeypatch.setattr(
        consumer_module.processor,
        "process",
        AsyncMock(side_effect=WebhookDeliveryError("unavailable")),
    )
    publish = AsyncMock()
    monkeypatch.setattr(consumer_module.broker, "publish", publish)
    message = MessageStub(retry_count=2)

    await consumer_module.handle_payment_message(
        {"payment_id": str(uuid4())},
        message,  # type: ignore[arg-type]
        make_logger(),
    )

    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()
    message.nack.assert_not_awaited()
    publish.assert_not_awaited()


async def test_consumer_requeues_original_if_retry_publish_fails(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        consumer_module.processor,
        "process",
        AsyncMock(side_effect=RuntimeError("processing failed")),
    )
    monkeypatch.setattr(consumer_module.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(
        consumer_module.broker,
        "publish",
        AsyncMock(side_effect=RuntimeError("broker unavailable")),
    )
    message = MessageStub()

    await consumer_module.handle_payment_message(
        {"payment_id": str(uuid4())},
        message,  # type: ignore[arg-type]
        make_logger(),
    )

    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()


@pytest.mark.parametrize(
    ("retry_count", "expected_attempt"),
    [
        (0, 1),
        (1, 2),
        ("2", 3),
        ("invalid", 1),
        (None, 1),
        (-10, 1),
    ],
)
def test_get_attempt_is_defensive(
    retry_count: object,
    expected_attempt: int,
) -> None:
    message = MessageStub(retry_count=retry_count)

    assert (
        consumer_module.get_attempt(message)  # type: ignore[arg-type]
        == expected_attempt
    )


async def test_invalid_payload_moves_directly_to_dlq(monkeypatch) -> None:
    sleep = AsyncMock()
    publish = AsyncMock()
    process = AsyncMock()
    monkeypatch.setattr(consumer_module.asyncio, "sleep", sleep)
    monkeypatch.setattr(consumer_module.broker, "publish", publish)
    monkeypatch.setattr(consumer_module.processor, "process", process)
    message = MessageStub()

    await consumer_module.handle_payment_message(
        {},
        message,  # type: ignore[arg-type]
        make_logger(),
    )

    process.assert_not_called()
    sleep.assert_not_awaited()
    publish.assert_not_awaited()
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


async def test_permanent_processing_error_moves_directly_to_dlq(
    monkeypatch,
) -> None:
    process = AsyncMock(side_effect=InvalidPaymentStateError("invalid payment state"))
    publish = AsyncMock()
    monkeypatch.setattr(consumer_module.processor, "process", process)
    monkeypatch.setattr(consumer_module.broker, "publish", publish)
    message = MessageStub()

    await consumer_module.handle_payment_message(
        {"payment_id": str(uuid4())},
        message,  # type: ignore[arg-type]
        make_logger(),
    )

    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()
    publish.assert_not_awaited()


async def test_active_webhook_claim_is_requeued_without_spending_attempt(
    monkeypatch,
) -> None:
    process = AsyncMock(
        side_effect=WebhookDeliveryInProgressError(
            retry_after_seconds=10,
        )
    )
    sleep = AsyncMock()
    publish = AsyncMock()
    monkeypatch.setattr(consumer_module.processor, "process", process)
    monkeypatch.setattr(consumer_module.asyncio, "sleep", sleep)
    monkeypatch.setattr(consumer_module.broker, "publish", publish)
    message = MessageStub(retry_count=2)

    await consumer_module.handle_payment_message(
        {"payment_id": str(uuid4())},
        message,  # type: ignore[arg-type]
        make_logger(),
    )

    sleep.assert_awaited_once_with(consumer_module.settings.webhook_claim_retry_seconds)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()
    publish.assert_not_awaited()


async def test_subscriber_delegates_to_message_handler(monkeypatch) -> None:
    handle = AsyncMock()
    monkeypatch.setattr(consumer_module, "handle_payment_message", handle)
    payload = {"payment_id": str(uuid4())}
    message = MessageStub()
    logger = make_logger()

    await consumer_module.handle_payment_created(
        payload,
        message,  # type: ignore[arg-type]
        logger,
    )

    handle.assert_awaited_once_with(payload, message, logger)


async def test_consumer_declares_and_binds_dlq(monkeypatch) -> None:
    declared_exchange = object()
    declared_queue = SimpleNamespace(bind=AsyncMock())
    declare_exchange = AsyncMock(return_value=declared_exchange)
    declare_queue = AsyncMock(return_value=declared_queue)
    monkeypatch.setattr(
        consumer_module.broker,
        "declare_exchange",
        declare_exchange,
    )
    monkeypatch.setattr(
        consumer_module.broker,
        "declare_queue",
        declare_queue,
    )

    await consumer_module.declare_dead_letter_topology()

    declare_exchange.assert_awaited_once_with(
        consumer_module.payment_dead_letter_exchange
    )
    declare_queue.assert_awaited_once_with(consumer_module.payment_dead_letter_queue)
    declared_queue.bind.assert_awaited_once_with(
        declared_exchange,
        routing_key=(consumer_module.settings.payment_dead_letter_routing_key),
    )


def test_payment_queue_has_dead_letter_arguments() -> None:
    assert consumer_module.payment_queue.arguments == {
        "x-queue-type": "classic",
        "x-dead-letter-exchange": (
            consumer_module.settings.payment_dead_letter_exchange
        ),
        "x-dead-letter-routing-key": (
            consumer_module.settings.payment_dead_letter_routing_key
        ),
    }
