from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import (
    IdempotencyConflictError,
    InvalidPaymentStateError,
    PaymentNotFoundError,
    WebhookDeliveryInProgressError,
)
from app.domain.enums import PaymentStatus
from app.models.outbox import Outbox
from app.models.payment import IDEMPOTENCY_KEY_CONSTRAINT, Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas.events import PaymentCreatedEvent
from app.schemas.payment import SPaymentCreate


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.payment_repository = PaymentRepository(session)
        self.outbox_repository = OutboxRepository(session)

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        payment = await self.payment_repository.get(payment_id)
        return payment

    async def create_payment(
        self, payload: SPaymentCreate, idempotency_key: str
    ) -> Payment:
        try:
            async with self.session.begin():
                existing_payment = await self.payment_repository.get_by_idempotency_key(
                    idempotency_key
                )

                if existing_payment is not None:
                    self._ensure_same_request(existing_payment, payload)
                    return existing_payment

                payment = Payment(
                    amount=payload.amount,
                    currency=payload.currency,
                    description=payload.description,
                    metadata_=payload.metadata,
                    idempotency_key=idempotency_key,
                    webhook_url=str(payload.webhook_url),
                )

                await self.payment_repository.create(payment)

                event = PaymentCreatedEvent(payment_id=payment.payment_id)

                outbox_event = Outbox(
                    event_type=PaymentCreatedEvent.event_type,
                    payload=event.model_dump(mode="json"),
                )

                await self.outbox_repository.create(outbox_event)

                return payment

        except IntegrityError as exc:
            if not self._is_idempotency_unique_violation(exc):
                raise

            async with self.session.begin():
                existing_payment = await self.payment_repository.get_by_idempotency_key(
                    idempotency_key
                )

                if existing_payment is None:
                    raise

                self._ensure_same_request(existing_payment, payload)

                return existing_payment

    @staticmethod
    def _ensure_same_request(payment: Payment, payload: SPaymentCreate) -> None:
        is_same_request = (
            payment.amount == payload.amount
            and payment.currency == payload.currency
            and payment.description == payload.description
            and payment.metadata_ == payload.metadata
            and payment.webhook_url == str(payload.webhook_url)
        )

        if not is_same_request:
            raise IdempotencyConflictError

    @staticmethod
    def _is_idempotency_unique_violation(error: IntegrityError) -> bool:
        sqlstate: str | None = None
        constraint_name: str | None = None
        current: BaseException | None = error.orig
        visited: set[int] = set()

        while current is not None and id(current) not in visited:
            visited.add(id(current))
            sqlstate = sqlstate or getattr(current, "sqlstate", None)
            sqlstate = sqlstate or getattr(current, "pgcode", None)
            constraint_name = constraint_name or getattr(
                current,
                "constraint_name",
                None,
            )
            current = current.__cause__ or current.__context__

        if constraint_name is None and IDEMPOTENCY_KEY_CONSTRAINT in str(error.orig):
            constraint_name = IDEMPOTENCY_KEY_CONSTRAINT

        return sqlstate == "23505" and constraint_name == IDEMPOTENCY_KEY_CONSTRAINT

    async def complete_payment(
        self, payment_id: UUID, status: PaymentStatus
    ) -> Payment | None:
        if status == PaymentStatus.PENDING:
            raise InvalidPaymentStateError(
                "Completed payment cannot have pending status"
            )

        async with self.session.begin():
            payment = await self.payment_repository.get(payment_id, for_update=True)

            if payment is None:
                raise PaymentNotFoundError(f"Payment {payment_id} not found")

            if payment.status != PaymentStatus.PENDING:
                return None

            payment.status = status
            payment.processed_at = datetime.now(UTC)

            return payment

    async def mark_webhook_sent(self, payment_id: UUID) -> Payment:
        async with self.session.begin():
            payment = await self.payment_repository.get(payment_id, for_update=True)

            if payment is None:
                raise PaymentNotFoundError(f"Payment {payment_id} not found")

            if payment.webhook_sent_at is None:
                payment.webhook_sent_at = datetime.now(UTC)
            payment.webhook_claimed_at = None

            return payment

    async def claim_webhook_delivery(
        self, payment_id: UUID, *, claim_ttl_seconds: float
    ) -> Payment | None:
        now = datetime.now(UTC)
        stale_before = now - timedelta(seconds=claim_ttl_seconds)

        async with self.session.begin():
            payment = await self.payment_repository.get(payment_id, for_update=True)

            if payment is None:
                raise PaymentNotFoundError(f"Payment {payment_id} not found")

            if payment.webhook_sent_at is not None:
                return None

            if (
                payment.webhook_claimed_at is not None
                and payment.webhook_claimed_at > stale_before
            ):
                claim_expires_at = payment.webhook_claimed_at + timedelta(
                    seconds=claim_ttl_seconds
                )
                retry_after_seconds = max(
                    (claim_expires_at - now).total_seconds(),
                    0.001,
                )
                raise WebhookDeliveryInProgressError(retry_after_seconds)

            payment.webhook_claimed_at = now
            return payment

    async def release_webhook_claim(self, payment_id: UUID) -> Payment:
        async with self.session.begin():
            payment = await self.payment_repository.get(payment_id, for_update=True)

            if payment is None:
                raise PaymentNotFoundError(f"Payment {payment_id} not found")

            if payment.webhook_sent_at is None:
                payment.webhook_claimed_at = None

            return payment
