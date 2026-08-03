from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import Outbox


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, outbox_event: Outbox) -> Outbox:
        self.session.add(outbox_event)
        await self.session.flush()
        return outbox_event

    async def get(self, outbox_id: UUID, *, for_update: bool = False) -> Outbox | None:
        return await self.session.get(Outbox, outbox_id, with_for_update=for_update)

    async def claim_unpublished_batch(
        self, *, limit: int, now: datetime, stale_before: datetime
    ) -> list[Outbox]:
        query = (
            select(Outbox)
            .where(
                Outbox.published_at.is_(None),
                Outbox.next_attempt_at <= now,
                or_(
                    Outbox.claimed_at.is_(None),
                    Outbox.claimed_at <= stale_before,
                ),
            )
            .order_by(
                Outbox.next_attempt_at,
                Outbox.created_at,
                Outbox.outbox_id,
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

        result = await self.session.scalars(query)
        outbox_events = list(result.all())

        for outbox_event in outbox_events:
            outbox_event.claimed_at = now

        return outbox_events

    @staticmethod
    def mark_as_published(outbox_event: Outbox, *, now: datetime | None = None) -> None:
        outbox_event.published_at = now or datetime.now(UTC)
        outbox_event.claimed_at = None
        outbox_event.last_error = None
        outbox_event.publish_attempts += 1

    @staticmethod
    def mark_as_failed(
        outbox_event: Outbox, *, error: str, next_attempt_at: datetime
    ) -> None:
        outbox_event.claimed_at = None
        outbox_event.last_error = error
        outbox_event.next_attempt_at = next_attempt_at
        outbox_event.publish_attempts += 1
