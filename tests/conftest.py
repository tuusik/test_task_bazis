import os
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://test:test@localhost:5432/test",
)
os.environ.setdefault(
    "RABBITMQ_URL",
    "amqp://guest:guest@localhost:5672/",
)

from app.core.config import Settings
from app.domain.enums import Currency, PaymentStatus
from app.models.payment import Payment
from app.schemas.payment import SPaymentCreate


class TransactionStub:
    def __init__(self) -> None:
        self.entered = False
        self.exited_with: type[BaseException] | None = None

    async def __aenter__(self) -> "TransactionStub":
        self.entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> bool:
        self.exited_with = exc_type
        return False


class SessionStub:
    def __init__(self) -> None:
        self.transactions: list[TransactionStub] = []

    def begin(self) -> TransactionStub:
        transaction = TransactionStub()
        self.transactions.append(transaction)
        return transaction


@pytest.fixture
def session_stub() -> SessionStub:
    return SessionStub()


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        _env_file=None,
        api_key="test-api-key",
        app_env="test",
        consumer_processing_time=(2, 5),
        consumer_process_chance=(90, 10),
    )


@pytest.fixture
def payment_payload() -> SPaymentCreate:
    return SPaymentCreate(
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        description="Test payment",
        metadata={"source": "pytest"},
        webhook_url="https://example.com/webhook",
    )


@pytest.fixture
def payment_factory() -> Callable[..., Payment]:
    def factory(
        *,
        payment_id: UUID | None = None,
        status: PaymentStatus = PaymentStatus.PENDING,
        description: str = "Test payment",
        idempotency_key: str = "test-idempotency-key",
        processed_at: datetime | None = None,
        webhook_sent_at: datetime | None = None,
        webhook_claimed_at: datetime | None = None,
        webhook_url: str = "https://example.com/webhook",
    ) -> Payment:
        return Payment(
            payment_id=payment_id or uuid4(),
            amount=Decimal("100.50"),
            currency=Currency.RUB,
            description=description,
            metadata_={"source": "pytest"},
            status=status,
            idempotency_key=idempotency_key,
            webhook_url=webhook_url,
            created_at=datetime.now(UTC),
            processed_at=processed_at,
            webhook_sent_at=webhook_sent_at,
            webhook_claimed_at=webhook_claimed_at,
        )

    return factory
