from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

import app.services.payment_processor as processor_module
from app.core.exceptions import (
    InvalidPaymentStateError,
    PaymentNotFoundError,
    WebhookDeliveryError,
)
from app.domain.enums import PaymentStatus
from app.schemas.events import PaymentCreatedEvent
from app.services.payment_processor import PaymentProcessor


class SessionContextStub:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *_args: object) -> None:
        return None


def make_processor(
    monkeypatch,
    service: object,
    *,
    final_status: PaymentStatus = PaymentStatus.SUCCEEDED,
) -> tuple[PaymentProcessor, AsyncMock, AsyncMock]:
    simulate = AsyncMock(return_value=final_status)
    send = AsyncMock()
    monkeypatch.setattr(
        processor_module,
        "PaymentService",
        lambda _session: service,
    )
    processor = PaymentProcessor(
        lambda: SessionContextStub(),  # type: ignore[arg-type]
        SimpleNamespace(simulate=simulate),  # type: ignore[arg-type]
        SimpleNamespace(send=send),  # type: ignore[arg-type]
        webhook_claim_ttl_seconds=30,
    )
    return processor, simulate, send


async def test_processor_completes_pending_payment_and_sends_webhook(
    monkeypatch,
    payment_factory,
) -> None:
    payment = payment_factory()
    processed_payment = payment_factory(
        payment_id=payment.payment_id,
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
    )
    delivered_payment = payment_factory(
        payment_id=payment.payment_id,
        status=PaymentStatus.SUCCEEDED,
        processed_at=processed_payment.processed_at,
        webhook_sent_at=datetime.now(UTC),
    )
    service = SimpleNamespace(
        get_payment=AsyncMock(return_value=payment),
        complete_payment=AsyncMock(return_value=processed_payment),
        claim_webhook_delivery=AsyncMock(return_value=processed_payment),
        release_webhook_claim=AsyncMock(),
        mark_webhook_sent=AsyncMock(return_value=delivered_payment),
    )
    processor, simulate, send = make_processor(monkeypatch, service)

    result = await processor.process(PaymentCreatedEvent(payment_id=payment.payment_id))

    assert result is delivered_payment
    simulate.assert_awaited_once_with()
    service.complete_payment.assert_awaited_once_with(
        payment.payment_id,
        PaymentStatus.SUCCEEDED,
    )
    send.assert_awaited_once()
    url, notification = send.await_args.args
    assert url == payment.webhook_url
    assert notification.payment_id == payment.payment_id
    assert notification.metadata == payment.metadata_
    service.claim_webhook_delivery.assert_awaited_once_with(
        payment.payment_id,
        claim_ttl_seconds=30,
    )
    service.mark_webhook_sent.assert_awaited_once_with(payment.payment_id)


async def test_processor_retry_sends_only_webhook_for_final_payment(
    monkeypatch,
    payment_factory,
) -> None:
    payment = payment_factory(
        status=PaymentStatus.FAILED,
        processed_at=datetime.now(UTC),
    )
    service = SimpleNamespace(
        get_payment=AsyncMock(return_value=payment),
        complete_payment=AsyncMock(),
        claim_webhook_delivery=AsyncMock(return_value=payment),
        release_webhook_claim=AsyncMock(),
        mark_webhook_sent=AsyncMock(return_value=payment),
    )
    processor, simulate, send = make_processor(monkeypatch, service)

    await processor.process(PaymentCreatedEvent(payment_id=payment.payment_id))

    simulate.assert_not_awaited()
    service.complete_payment.assert_not_awaited()
    send.assert_awaited_once()
    service.mark_webhook_sent.assert_awaited_once()


async def test_processor_skips_already_delivered_payment(
    monkeypatch,
    payment_factory,
) -> None:
    payment = payment_factory(
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
        webhook_sent_at=datetime.now(UTC),
    )
    service = SimpleNamespace(
        get_payment=AsyncMock(return_value=payment),
        complete_payment=AsyncMock(),
        claim_webhook_delivery=AsyncMock(),
        release_webhook_claim=AsyncMock(),
        mark_webhook_sent=AsyncMock(),
    )
    processor, simulate, send = make_processor(monkeypatch, service)

    result = await processor.process(PaymentCreatedEvent(payment_id=payment.payment_id))

    assert result is payment
    simulate.assert_not_awaited()
    send.assert_not_awaited()
    service.claim_webhook_delivery.assert_not_awaited()
    service.mark_webhook_sent.assert_not_awaited()


async def test_processor_does_not_mark_failed_webhook_as_sent(
    monkeypatch,
    payment_factory,
) -> None:
    payment = payment_factory(
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
    )
    service = SimpleNamespace(
        get_payment=AsyncMock(return_value=payment),
        complete_payment=AsyncMock(),
        claim_webhook_delivery=AsyncMock(return_value=payment),
        release_webhook_claim=AsyncMock(),
        mark_webhook_sent=AsyncMock(),
    )
    processor, _, send = make_processor(monkeypatch, service)
    send.side_effect = WebhookDeliveryError("unavailable")

    with pytest.raises(WebhookDeliveryError):
        await processor.process(PaymentCreatedEvent(payment_id=payment.payment_id))

    service.mark_webhook_sent.assert_not_awaited()
    service.release_webhook_claim.assert_awaited_once_with(payment.payment_id)


async def test_processor_rejects_missing_payment(monkeypatch) -> None:
    payment_id = uuid4()
    service = SimpleNamespace(
        get_payment=AsyncMock(return_value=None),
        complete_payment=AsyncMock(),
        claim_webhook_delivery=AsyncMock(),
        release_webhook_claim=AsyncMock(),
        mark_webhook_sent=AsyncMock(),
    )
    processor, _, _ = make_processor(monkeypatch, service)

    with pytest.raises(PaymentNotFoundError):
        await processor.process(PaymentCreatedEvent(payment_id=payment_id))


async def test_processor_rejects_final_payment_without_timestamp(
    monkeypatch,
    payment_factory,
) -> None:
    payment = payment_factory(status=PaymentStatus.SUCCEEDED)
    service = SimpleNamespace(
        get_payment=AsyncMock(return_value=payment),
        complete_payment=AsyncMock(),
        claim_webhook_delivery=AsyncMock(),
        release_webhook_claim=AsyncMock(),
        mark_webhook_sent=AsyncMock(),
    )
    processor, _, send = make_processor(monkeypatch, service)

    with pytest.raises(InvalidPaymentStateError, match="no processing timestamp"):
        await processor.process(PaymentCreatedEvent(payment_id=payment.payment_id))

    send.assert_not_awaited()


async def test_processor_refetches_payment_completed_by_duplicate_consumer(
    monkeypatch,
    payment_factory,
) -> None:
    pending = payment_factory()
    completed = payment_factory(
        payment_id=pending.payment_id,
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
    )
    service = SimpleNamespace(
        get_payment=AsyncMock(side_effect=[pending, completed]),
        complete_payment=AsyncMock(return_value=None),
        claim_webhook_delivery=AsyncMock(return_value=completed),
        release_webhook_claim=AsyncMock(),
        mark_webhook_sent=AsyncMock(return_value=completed),
    )
    processor, _, send = make_processor(monkeypatch, service)

    await processor.process(PaymentCreatedEvent(payment_id=pending.payment_id))

    assert service.get_payment.await_count == 2
    send.assert_awaited_once()
