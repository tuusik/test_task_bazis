from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import Outbox
from app.repositories.outbox import OutboxRepository
from app.schemas.events import PaymentCreatedEvent


class EventPublisher(Protocol):
    async def publish(
        self,
        message: PaymentCreatedEvent,
        **kwargs: Any,
    ) -> Any: ...


class OutboxService:
    def __init__(
        self,
        session: AsyncSession,
        publisher: EventPublisher,
        *,
        claim_ttl_seconds: float,
        publish_timeout_seconds: float,
        retry_base_seconds: float,
        max_backoff_seconds: float,
    ) -> None:
        self.session = session
        self.publisher = publisher
        self.repository = OutboxRepository(session)
        self.claim_ttl_seconds = claim_ttl_seconds
        self.publish_timeout_seconds = publish_timeout_seconds
        self.retry_base_seconds = retry_base_seconds
        self.max_backoff_seconds = max_backoff_seconds

    async def publish_batch(self, limit: int) -> int:
        published_count = 0

        for _ in range(limit):
            outbox_event = await self._claim_next()
            if outbox_event is None:
                break

            try:
                await self._publish_event(outbox_event)
            except Exception as exc:
                await self._mark_as_failed(outbox_event, exc)
                continue

            await self._mark_as_published(outbox_event)
            published_count += 1

        return published_count

    async def _claim_next(self) -> Outbox | None:
        now = datetime.now(UTC)
        stale_before = now - timedelta(seconds=self.claim_ttl_seconds)

        async with self.session.begin():
            events = await self.repository.claim_unpublished_batch(
                limit=1,
                now=now,
                stale_before=stale_before,
            )

        return events[0] if events else None

    async def _publish_event(self, outbox_event: Outbox) -> None:
        if outbox_event.event_type != PaymentCreatedEvent.event_type:
            raise ValueError(
                f"Unsupported outbox event type: {outbox_event.event_type}"
            )

        if outbox_event.outbox_id is None:
            raise ValueError("Persisted outbox event has no identifier")

        event = PaymentCreatedEvent.model_validate(outbox_event.payload)

        await self.publisher.publish(
            event,
            message_id=str(outbox_event.outbox_id),
            timeout=self.publish_timeout_seconds,
        )

    async def _mark_as_published(self, outbox_event: Outbox) -> None:
        async with self.session.begin():
            current_event = await self.repository.get(
                outbox_event.outbox_id, for_update=True
            )
            if current_event is None:
                raise ValueError(f"Outbox event {outbox_event.outbox_id} disappeared")

            if current_event.published_at is None:
                self.repository.mark_as_published(current_event)

    async def _mark_as_failed(self, outbox_event: Outbox, error: Exception) -> None:
        async with self.session.begin():
            current_event = await self.repository.get(
                outbox_event.outbox_id, for_update=True
            )
            if current_event is None or current_event.published_at is not None:
                return

            attempt = current_event.publish_attempts + 1
            exponent = min(attempt - 1, 30)
            delay = min(
                self.retry_base_seconds * (2**exponent), self.max_backoff_seconds
            )
            next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
            error_message = f"{type(error).__name__}: {error}"[:2000]

            self.repository.mark_as_failed(
                current_event, error=error_message, next_attempt_at=next_attempt_at
            )
