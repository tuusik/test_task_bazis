from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(
        self, payment_id: UUID, *, for_update: bool = False
    ) -> Payment | None:
        return await self.session.get(Payment, payment_id, with_for_update=for_update)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        query = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return await self.session.scalar(query)

    async def create(self, payment: Payment) -> Payment:
        self.session.add(payment)
        await self.session.flush()
        await self.session.refresh(payment)
        return payment
