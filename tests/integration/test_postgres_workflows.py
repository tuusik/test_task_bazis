import asyncio
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import func, select

from app.core.exceptions import WebhookDeliveryInProgressError
from app.domain.enums import Currency, PaymentStatus
from app.models.outbox import Outbox
from app.models.payment import Payment
from app.repositories.payments import PaymentRepository
from app.schemas.events import PaymentCreatedEvent
from app.schemas.payment import SPaymentCreate
from app.services.outbox import OutboxService
from app.services.payment_processor import PaymentProcessor
from app.services.payments import PaymentService

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION_TESTS") != "1",
        reason="Set RUN_INTEGRATION_TESTS=1 to run integration tests",
    ),
]


class FirstLookupBarrier:
    def __init__(self, participants: int) -> None:
        self.participants = participants
        self.arrived = 0
        self.condition = asyncio.Condition()

    async def wait(self) -> None:
        async with self.condition:
            self.arrived += 1
            if self.arrived >= self.participants:
                self.condition.notify_all()
                return

            await self.condition.wait_for(lambda: self.arrived >= self.participants)


class BarrierPaymentRepository(PaymentRepository):
    def __init__(self, session, barrier: FirstLookupBarrier) -> None:
        super().__init__(session)
        self.barrier = barrier
        self.first_lookup = True

    async def get_by_idempotency_key(
        self,
        idempotency_key: str,
    ) -> Payment | None:
        payment = await super().get_by_idempotency_key(idempotency_key)
        if self.first_lookup and payment is None:
            self.first_lookup = False
            await self.barrier.wait()
        return payment


class NeverSimulator:
    async def simulate(self) -> PaymentStatus:
        raise AssertionError("A final payment must not be simulated again")


class BlockingWebhookClient:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def send(self, _url: str, _notification: Any) -> None:
        self.calls += 1
        self.started.set()
        await self.release.wait()


class LockCheckingPublisher:
    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory
        self.calls: list[str] = []

    async def publish(self, _message: Any, **kwargs: Any) -> None:
        message_id = kwargs["message_id"]
        async with self.session_factory() as session, session.begin():
            event = await session.get(
                Outbox,
                UUID(message_id),
                with_for_update={"nowait": True},
            )
            assert event is not None
        self.calls.append(message_id)


class BlockingBatchPublisher:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls: list[str] = []

    async def publish(self, _message: Any, **kwargs: Any) -> None:
        self.calls.append(kwargs["message_id"])
        if len(self.calls) == 1:
            self.started.set()
            await self.release.wait()


async def test_concurrent_idempotent_requests_create_one_payment_and_event(
    integration_session_factory,
) -> None:
    payload = SPaymentCreate(
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        description="Concurrent payment",
        metadata={"source": "integration"},
        webhook_url="https://example.com/webhook",
    )
    barrier = FirstLookupBarrier(participants=2)

    async def create_payment() -> Payment:
        async with integration_session_factory() as session:
            service = PaymentService(session)
            service.payment_repository = BarrierPaymentRepository(
                session,
                barrier,
            )
            return await service.create_payment(payload, "concurrent-key")

    first, second = await asyncio.gather(
        create_payment(),
        create_payment(),
    )

    assert first.payment_id == second.payment_id

    async with integration_session_factory() as session:
        payment_count = await session.scalar(select(func.count()).select_from(Payment))
        outbox_count = await session.scalar(select(func.count()).select_from(Outbox))

    assert payment_count == 1
    assert outbox_count == 1


async def test_webhook_claim_prevents_concurrent_delivery(
    integration_session_factory,
) -> None:
    payment = Payment(
        amount=Decimal("42.00"),
        currency=Currency.USD,
        description="Claim integration test",
        metadata_={},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="claim-key",
        webhook_url="https://example.com/webhook",
        processed_at=datetime.now(UTC),
    )
    async with integration_session_factory() as session, session.begin():
        session.add(payment)

    webhook_client = BlockingWebhookClient()
    processor = PaymentProcessor(
        integration_session_factory,
        NeverSimulator(),  # type: ignore[arg-type]
        webhook_client,  # type: ignore[arg-type]
        webhook_claim_ttl_seconds=30,
    )
    event = PaymentCreatedEvent(payment_id=payment.payment_id)

    first_delivery = asyncio.create_task(processor.process(event))
    await asyncio.wait_for(webhook_client.started.wait(), timeout=5)

    with pytest.raises(WebhookDeliveryInProgressError):
        await processor.process(event)

    webhook_client.release.set()
    await asyncio.wait_for(first_delivery, timeout=5)

    async with integration_session_factory() as session:
        stored_payment = await session.get(Payment, payment.payment_id)

    assert webhook_client.calls == 1
    assert stored_payment is not None
    assert stored_payment.webhook_sent_at is not None
    assert stored_payment.webhook_claimed_at is None


async def test_outbox_failure_does_not_block_following_event_or_hold_lock(
    integration_session_factory,
) -> None:
    now = datetime.now(UTC)
    invalid_event = Outbox(
        event_type="unsupported.event",
        payload={},
        created_at=now,
        next_attempt_at=now,
    )
    valid_event = Outbox(
        event_type=PaymentCreatedEvent.event_type,
        payload={"payment_id": "00000000-0000-0000-0000-000000000001"},
        created_at=now + timedelta(microseconds=1),
        next_attempt_at=now,
    )
    async with integration_session_factory() as session, session.begin():
        session.add_all([invalid_event, valid_event])

    publisher = LockCheckingPublisher(integration_session_factory)
    async with integration_session_factory() as session:
        service = OutboxService(
            session,
            publisher,
            claim_ttl_seconds=30,
            publish_timeout_seconds=5,
            retry_base_seconds=1,
            max_backoff_seconds=60,
        )
        published_count = await service.publish_batch(limit=10)

    async with integration_session_factory() as session:
        stored_invalid = await session.get(Outbox, invalid_event.outbox_id)
        stored_valid = await session.get(Outbox, valid_event.outbox_id)

    assert published_count == 1
    assert publisher.calls == [str(valid_event.outbox_id)]
    assert stored_invalid is not None
    assert stored_invalid.published_at is None
    assert stored_invalid.claimed_at is None
    assert stored_invalid.publish_attempts == 1
    assert "Unsupported outbox event type" in stored_invalid.last_error
    assert stored_invalid.next_attempt_at > now
    assert stored_valid is not None
    assert stored_valid.published_at is not None
    assert stored_valid.claimed_at is None
    assert stored_valid.publish_attempts == 1


async def test_outbox_claims_later_events_just_in_time(
    integration_session_factory,
) -> None:
    now = datetime.now(UTC)
    first_event = Outbox(
        event_type=PaymentCreatedEvent.event_type,
        payload={"payment_id": "00000000-0000-0000-0000-000000000001"},
        created_at=now,
        next_attempt_at=now,
    )
    second_event = Outbox(
        event_type=PaymentCreatedEvent.event_type,
        payload={"payment_id": "00000000-0000-0000-0000-000000000002"},
        created_at=now + timedelta(microseconds=1),
        next_attempt_at=now,
    )
    async with integration_session_factory() as session, session.begin():
        session.add_all([first_event, second_event])

    publisher = BlockingBatchPublisher()
    async with integration_session_factory() as session:
        service = OutboxService(
            session,
            publisher,
            claim_ttl_seconds=30,
            publish_timeout_seconds=5,
            retry_base_seconds=1,
            max_backoff_seconds=60,
        )
        publishing = asyncio.create_task(service.publish_batch(limit=2))
        await asyncio.wait_for(publisher.started.wait(), timeout=5)

        async with integration_session_factory() as inspection_session:
            stored_first = await inspection_session.get(
                Outbox,
                first_event.outbox_id,
            )
            stored_second = await inspection_session.get(
                Outbox,
                second_event.outbox_id,
            )

        assert stored_first is not None
        assert stored_first.claimed_at is not None
        assert stored_second is not None
        assert stored_second.claimed_at is None

        publisher.release.set()
        assert await asyncio.wait_for(publishing, timeout=5) == 2

    assert publisher.calls == [
        str(first_event.outbox_id),
        str(second_event.outbox_id),
    ]
