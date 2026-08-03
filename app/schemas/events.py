from decimal import Decimal
from typing import Any, ClassVar
from uuid import UUID

from pydantic import AwareDatetime

from app.domain.enums import Currency, PaymentStatus
from app.schemas.base import SBase


class PaymentCreatedEvent(SBase):
    event_type: ClassVar[str] = "payment.created"

    payment_id: UUID


class WebhookNotification(SBase):
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: AwareDatetime
    metadata: dict[str, Any]
