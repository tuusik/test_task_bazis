from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import AliasChoices, AwareDatetime, Field, HttpUrl, UrlConstraints

from app.domain.enums import Currency, PaymentStatus
from app.schemas.base import SBase

WebhookUrl = Annotated[HttpUrl, UrlConstraints(max_length=2048)]


class SPaymentDetail(SBase):
    payment_id: UUID
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(min_length=1, max_length=100)
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("metadata_", "metadata", "meta"),
    )
    status: PaymentStatus
    idempotency_key: str
    webhook_url: WebhookUrl
    created_at: AwareDatetime
    processed_at: AwareDatetime | None
    webhook_sent_at: AwareDatetime | None


class SPaymentCreate(SBase):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(min_length=1, max_length=100)
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("metadata", "meta"),
    )
    webhook_url: WebhookUrl


class SPaymentResponse(SBase):
    payment_id: UUID
    status: PaymentStatus
    created_at: AwareDatetime
