from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.deps import get_payment_service
from app.core.config import get_settings
from app.core.exceptions import IdempotencyConflictError
from app.main import app


class PaymentServiceStub:
    def __init__(self, payment) -> None:
        self.payment = payment
        self.created_with: tuple[object, str] | None = None
        self.raise_conflict = False
        self.get_payment = AsyncMock(return_value=payment)

    async def create_payment(self, payload, idempotency_key: str):
        self.created_with = (payload, idempotency_key)
        if self.raise_conflict:
            raise IdempotencyConflictError
        return self.payment


@pytest.fixture
def payment_service_stub(payment_factory) -> PaymentServiceStub:
    return PaymentServiceStub(payment_factory())


@pytest.fixture
async def client(
    test_settings,
    payment_service_stub,
):
    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_payment_service] = lambda: payment_service_stub
    transport = ASGITransport(app=app)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as http_client:
        yield http_client

    app.dependency_overrides.clear()


def auth_headers(**extra: str) -> dict[str, str]:
    return {"X-API-Key": "test-api-key", **extra}


async def test_api_rejects_missing_and_invalid_api_key(client) -> None:
    missing = await client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000")
    invalid = await client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000",
        headers={"X-API-Key": "wrong"},
    )

    assert missing.status_code == 401
    assert invalid.status_code == 401


async def test_get_payment_returns_payment(client, payment_service_stub) -> None:
    payment = payment_service_stub.payment

    response = await client.get(
        f"/api/v1/payments/{payment.payment_id}",
        headers=auth_headers(),
    )

    assert response.status_code == 200
    assert response.json()["payment_id"] == str(payment.payment_id)
    assert response.json()["status"] == "pending"
    assert response.json()["metadata"] == {"source": "pytest"}
    assert "meta" not in response.json()
    assert response.json()["webhook_sent_at"] is None


async def test_get_payment_returns_404(client, payment_service_stub) -> None:
    payment_service_stub.get_payment.return_value = None

    response = await client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000",
        headers=auth_headers(),
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Payment not found"}


async def test_create_payment_returns_202_and_strips_idempotency_key(
    client,
    payment_service_stub,
) -> None:
    response = await client.post(
        "/api/v1/payments",
        headers=auth_headers(**{"Idempotency-Key": "  request-key  "}),
        json={
            "amount": "100.50",
            "currency": "RUB",
            "description": "Test payment",
            "metadata": {"source": "pytest"},
            "webhook_url": "https://example.com/webhook",
        },
    )

    assert response.status_code == 202
    assert response.json()["payment_id"] == str(payment_service_stub.payment.payment_id)
    assert payment_service_stub.created_with is not None
    assert payment_service_stub.created_with[1] == "request-key"


async def test_create_payment_requires_idempotency_key(client) -> None:
    response = await client.post(
        "/api/v1/payments",
        headers=auth_headers(),
        json={
            "amount": "100.50",
            "currency": "RUB",
            "description": "Test payment",
            "metadata": {},
            "webhook_url": "https://example.com/webhook",
        },
    )

    assert response.status_code == 422


async def test_create_payment_returns_409_for_conflicting_key(
    client,
    payment_service_stub,
) -> None:
    payment_service_stub.raise_conflict = True

    response = await client.post(
        "/api/v1/payments",
        headers=auth_headers(**{"Idempotency-Key": "reused-key"}),
        json={
            "amount": "100.50",
            "currency": "RUB",
            "description": "Test payment",
            "metadata": {},
            "webhook_url": "https://example.com/webhook",
        },
    )

    assert response.status_code == 409
    assert "different request" in response.json()["detail"]
