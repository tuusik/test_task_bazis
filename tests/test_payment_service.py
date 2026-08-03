from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from app.core.exceptions import (
    IdempotencyConflictError,
    InvalidPaymentStateError,
    PaymentNotFoundError,
    WebhookDeliveryInProgressError,
)
from app.domain.enums import PaymentStatus
from app.models.payment import IDEMPOTENCY_KEY_CONSTRAINT
from app.schemas.events import PaymentCreatedEvent
from app.services.payments import PaymentService


class UniqueViolationStub(Exception):
    sqlstate = "23505"
    constraint_name = IDEMPOTENCY_KEY_CONSTRAINT


def make_service(session_stub: object) -> tuple[PaymentService, object, object]:
    service = PaymentService(session_stub)  # type: ignore[arg-type]
    payment_repository = SimpleNamespace(
        get=AsyncMock(),
        get_by_idempotency_key=AsyncMock(),
        create=AsyncMock(),
    )
    outbox_repository = SimpleNamespace(create=AsyncMock())
    service.payment_repository = payment_repository
    service.outbox_repository = outbox_repository
    return service, payment_repository, outbox_repository


async def test_create_payment_saves_payment_and_outbox_atomically(
    session_stub,
    payment_payload,
) -> None:
    service, payment_repository, outbox_repository = make_service(session_stub)
    payment_repository.get_by_idempotency_key.return_value = None

    async def create_payment(payment):
        payment.payment_id = uuid4()
        return payment

    payment_repository.create.side_effect = create_payment

    payment = await service.create_payment(payment_payload, "new-key")

    assert payment.idempotency_key == "new-key"
    payment_repository.create.assert_awaited_once_with(payment)
    outbox_repository.create.assert_awaited_once()
    outbox_event = outbox_repository.create.await_args.args[0]
    assert outbox_event.event_type == PaymentCreatedEvent.event_type
    assert outbox_event.payload == {"payment_id": str(payment.payment_id)}
    assert len(session_stub.transactions) == 1
    assert session_stub.transactions[0].exited_with is None


async def test_create_payment_returns_existing_payment_without_new_outbox(
    session_stub,
    payment_payload,
    payment_factory,
) -> None:
    service, payment_repository, outbox_repository = make_service(session_stub)
    existing_payment = payment_factory()
    payment_repository.get_by_idempotency_key.return_value = existing_payment

    result = await service.create_payment(payment_payload, "test-idempotency-key")

    assert result is existing_payment
    payment_repository.create.assert_not_awaited()
    outbox_repository.create.assert_not_awaited()


async def test_create_payment_rejects_reused_key_with_different_payload(
    session_stub,
    payment_payload,
    payment_factory,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment_repository.get_by_idempotency_key.return_value = payment_factory(
        description="Another request"
    )

    with pytest.raises(IdempotencyConflictError):
        await service.create_payment(payment_payload, "test-idempotency-key")


async def test_create_payment_handles_concurrent_unique_violation(
    session_stub,
    payment_payload,
    payment_factory,
) -> None:
    service, payment_repository, outbox_repository = make_service(session_stub)
    existing_payment = payment_factory()
    payment_repository.get_by_idempotency_key.side_effect = [
        None,
        existing_payment,
    ]
    payment_repository.create.side_effect = IntegrityError(
        "INSERT INTO payments ...",
        {},
        UniqueViolationStub("duplicate key"),
    )

    result = await service.create_payment(payment_payload, "test-idempotency-key")

    assert result is existing_payment
    assert len(session_stub.transactions) == 2
    assert session_stub.transactions[0].exited_with is IntegrityError
    outbox_repository.create.assert_not_awaited()


async def test_create_payment_does_not_hide_other_integrity_errors(
    session_stub,
    payment_payload,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment_repository.get_by_idempotency_key.return_value = None
    unrelated_error = IntegrityError(
        "INSERT INTO payments ...",
        {},
        Exception("check constraint failed"),
    )
    payment_repository.create.side_effect = unrelated_error

    with pytest.raises(IntegrityError) as raised:
        await service.create_payment(payment_payload, "new-key")

    assert raised.value is unrelated_error
    assert len(session_stub.transactions) == 1


async def test_complete_payment_sets_final_status_and_processed_at(
    session_stub,
    payment_factory,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment = payment_factory()
    payment_repository.get.return_value = payment

    result = await service.complete_payment(
        payment.payment_id,
        PaymentStatus.SUCCEEDED,
    )

    assert result is payment
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.processed_at.tzinfo is UTC
    payment_repository.get.assert_awaited_once_with(
        payment.payment_id,
        for_update=True,
    )


async def test_complete_payment_skips_already_processed_payment(
    session_stub,
    payment_factory,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment_repository.get.return_value = payment_factory(status=PaymentStatus.FAILED)

    result = await service.complete_payment(
        uuid4(),
        PaymentStatus.SUCCEEDED,
    )

    assert result is None


async def test_complete_payment_rejects_missing_payment(
    session_stub,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment_repository.get.return_value = None

    with pytest.raises(PaymentNotFoundError):
        await service.complete_payment(uuid4(), PaymentStatus.FAILED)


async def test_complete_payment_rejects_pending_as_final_status(
    session_stub,
) -> None:
    service, payment_repository, _ = make_service(session_stub)

    with pytest.raises(InvalidPaymentStateError, match="cannot have pending status"):
        await service.complete_payment(uuid4(), PaymentStatus.PENDING)

    payment_repository.get.assert_not_awaited()


async def test_mark_webhook_sent_sets_timestamp_once(
    session_stub,
    payment_factory,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment = payment_factory(
        status=PaymentStatus.SUCCEEDED,
        webhook_claimed_at=datetime.now(UTC),
    )
    payment_repository.get.return_value = payment

    first_result = await service.mark_webhook_sent(payment.payment_id)
    first_timestamp = payment.webhook_sent_at
    second_result = await service.mark_webhook_sent(payment.payment_id)

    assert first_result is payment
    assert second_result is payment
    assert first_timestamp is not None
    assert first_timestamp.tzinfo is UTC
    assert payment.webhook_sent_at is first_timestamp
    assert payment.webhook_claimed_at is None
    assert payment_repository.get.await_count == 2
    payment_repository.get.assert_awaited_with(
        payment.payment_id,
        for_update=True,
    )


async def test_mark_webhook_sent_rejects_missing_payment(
    session_stub,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment_repository.get.return_value = None

    with pytest.raises(PaymentNotFoundError):
        await service.mark_webhook_sent(uuid4())


async def test_claim_webhook_delivery_sets_claim_timestamp(
    session_stub,
    payment_factory,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment = payment_factory(
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
    )
    payment_repository.get.return_value = payment

    result = await service.claim_webhook_delivery(
        payment.payment_id,
        claim_ttl_seconds=30,
    )

    assert result is payment
    assert payment.webhook_claimed_at is not None
    assert payment.webhook_claimed_at.tzinfo is UTC


async def test_claim_webhook_delivery_rejects_active_claim(
    session_stub,
    payment_factory,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment = payment_factory(
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
        webhook_claimed_at=datetime.now(UTC),
    )
    payment_repository.get.return_value = payment

    with pytest.raises(WebhookDeliveryInProgressError) as raised:
        await service.claim_webhook_delivery(
            payment.payment_id,
            claim_ttl_seconds=30,
        )

    assert 0 < raised.value.retry_after_seconds <= 30


async def test_claim_webhook_delivery_recovers_stale_claim(
    session_stub,
    payment_factory,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    stale_claim = datetime.now(UTC) - timedelta(seconds=60)
    payment = payment_factory(
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
        webhook_claimed_at=stale_claim,
    )
    payment_repository.get.return_value = payment

    result = await service.claim_webhook_delivery(
        payment.payment_id,
        claim_ttl_seconds=30,
    )

    assert result is payment
    assert payment.webhook_claimed_at is not None
    assert payment.webhook_claimed_at > stale_claim


async def test_claim_webhook_delivery_skips_sent_webhook(
    session_stub,
    payment_factory,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment = payment_factory(
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
        webhook_sent_at=datetime.now(UTC),
    )
    payment_repository.get.return_value = payment

    result = await service.claim_webhook_delivery(
        payment.payment_id,
        claim_ttl_seconds=30,
    )

    assert result is None


async def test_release_webhook_claim_clears_unsent_claim(
    session_stub,
    payment_factory,
) -> None:
    service, payment_repository, _ = make_service(session_stub)
    payment = payment_factory(
        status=PaymentStatus.SUCCEEDED,
        webhook_claimed_at=datetime.now(UTC),
    )
    payment_repository.get.return_value = payment

    result = await service.release_webhook_claim(payment.payment_id)

    assert result is payment
    assert payment.webhook_claimed_at is None
