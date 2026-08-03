from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.domain.enums import Currency, PaymentStatus
from app.models.payment import Payment
from app.schemas.events import WebhookNotification
from app.schemas.payment import SPaymentCreate


def test_payment_create_accepts_valid_payload() -> None:
    payload = SPaymentCreate(
        amount="10.25",
        currency="USD",
        description="  Invoice payment  ",
        metadata={"invoice_id": 42},
        webhook_url="https://example.com/webhook",
    )

    assert payload.amount == Decimal("10.25")
    assert payload.currency is Currency.USD
    assert payload.description == "Invoice payment"
    assert payload.metadata == {"invoice_id": 42}


def test_payment_create_accepts_legacy_meta_alias_but_serializes_metadata() -> None:
    payload = SPaymentCreate.model_validate(
        {
            "amount": "10.25",
            "currency": "USD",
            "description": "Legacy client",
            "meta": {"invoice_id": 42},
            "webhook_url": "https://example.com/webhook",
        }
    )

    assert payload.metadata == {"invoice_id": 42}
    assert payload.model_dump(mode="json")["metadata"] == {"invoice_id": 42}
    assert "meta" not in payload.model_dump(mode="json")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "10.001"),
        ("amount", "12345678901234567.89"),
        ("currency", "GBP"),
        ("description", "   "),
        ("webhook_url", "not-a-url"),
    ],
)
def test_payment_create_rejects_invalid_values(
    field: str,
    value: object,
) -> None:
    data = {
        "amount": "10.25",
        "currency": "RUB",
        "description": "Payment",
        "metadata": {},
        "webhook_url": "https://example.com/webhook",
    }
    data[field] = value

    with pytest.raises(ValidationError):
        SPaymentCreate.model_validate(data)


def test_payment_create_forbids_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        SPaymentCreate.model_validate(
            {
                "amount": "10.25",
                "currency": "RUB",
                "description": "Payment",
                "metadata": {},
                "webhook_url": "https://example.com/webhook",
                "unexpected": True,
            }
        )


def test_webhook_url_length_matches_database_limit() -> None:
    prefix = "https://example.com/"
    accepted_url = prefix + "a" * (2048 - len(prefix))
    rejected_url = prefix + "a" * (2049 - len(prefix))

    payload = SPaymentCreate(
        amount="10.25",
        currency="RUB",
        description="Payment",
        metadata={},
        webhook_url=accepted_url,
    )

    assert len(str(payload.webhook_url)) == 2048

    with pytest.raises(ValidationError):
        SPaymentCreate(
            amount="10.25",
            currency="RUB",
            description="Payment",
            metadata={},
            webhook_url=rejected_url,
        )


def test_webhook_notification_serializes_external_payload() -> None:
    notification = WebhookNotification(
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("10.25"),
        currency=Currency.USD,
        processed_at=datetime.now(UTC),
        metadata={"invoice_id": 42},
    )

    serialized = notification.model_dump(mode="json")

    assert serialized["status"] == "succeeded"
    assert serialized["amount"] == "10.25"
    assert serialized["currency"] == "USD"
    assert serialized["metadata"] == {"invoice_id": 42}


def test_payment_amount_column_has_fixed_precision_and_scale() -> None:
    amount_type = Payment.__table__.c.amount.type

    assert amount_type.precision == 18
    assert amount_type.scale == 2
