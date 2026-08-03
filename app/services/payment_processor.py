from contextlib import suppress
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import InvalidPaymentStateError, PaymentNotFoundError
from app.domain.enums import PaymentStatus
from app.integrations.webhook import WebhookClient
from app.models.payment import Payment
from app.schemas.events import PaymentCreatedEvent, WebhookNotification
from app.services.payment_simulator import PaymentSimulator
from app.services.payments import PaymentService


class PaymentProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        simulator: PaymentSimulator,
        webhook_client: WebhookClient,
        *,
        webhook_claim_ttl_seconds: float,
    ) -> None:
        self.session_factory = session_factory
        self.simulator = simulator
        self.webhook_client = webhook_client
        self.webhook_claim_ttl_seconds = webhook_claim_ttl_seconds

    async def process(self, event: PaymentCreatedEvent) -> Payment:
        payment = await self._get_payment(event.payment_id)

        if payment.webhook_sent_at is not None:
            return payment

        if payment.status == PaymentStatus.PENDING:
            final_status = await self.simulator.simulate()
            payment = await self._complete_payment(event.payment_id, final_status)

        if payment.processed_at is None:
            raise InvalidPaymentStateError(
                f"Payment {event.payment_id} has no processing timestamp"
            )

        payment = await self._claim_webhook_delivery(payment.payment_id)

        if payment is None:
            return await self._get_payment(event.payment_id)

        try:
            if payment.processed_at is None:
                raise InvalidPaymentStateError(
                    f"Payment {event.payment_id} has no processing timestamp"
                )

            notification = WebhookNotification(
                payment_id=payment.payment_id,
                status=payment.status,
                amount=payment.amount,
                currency=payment.currency,
                processed_at=payment.processed_at,
                metadata=payment.metadata_,
            )

            await self.webhook_client.send(payment.webhook_url, notification)
        except Exception:
            with suppress(Exception):
                await self._release_webhook_claim(payment.payment_id)
            raise

        async with self.session_factory() as session:
            service = PaymentService(session)
            payment = await service.mark_webhook_sent(payment.payment_id)

        return payment

    async def _claim_webhook_delivery(self, payment_id: UUID) -> Payment | None:
        async with self.session_factory() as session:
            service = PaymentService(session)
            return await service.claim_webhook_delivery(
                payment_id, claim_ttl_seconds=self.webhook_claim_ttl_seconds
            )

    async def _release_webhook_claim(self, payment_id: UUID) -> None:
        async with self.session_factory() as session:
            service = PaymentService(session)
            await service.release_webhook_claim(payment_id)

    async def _get_payment(self, payment_id: UUID) -> Payment:
        async with self.session_factory() as session:
            service = PaymentService(session)
            payment = await service.get_payment(payment_id)

        if payment is None:
            raise PaymentNotFoundError(f"Payment {payment_id} not found")

        return payment

    async def _complete_payment(
        self, payment_id: UUID, status: PaymentStatus
    ) -> Payment:
        async with self.session_factory() as session:
            service = PaymentService(session)
            payment = await service.complete_payment(payment_id, status)

        if payment is not None:
            return payment

        return await self._get_payment(payment_id)
