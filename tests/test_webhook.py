import json
import socket
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from app.core.exceptions import (
    PermanentWebhookDeliveryError,
    WebhookDeliveryError,
)
from app.domain.enums import Currency, PaymentStatus
from app.integrations.webhook import WebhookClient
from app.schemas.events import WebhookNotification


async def public_resolver(_host: str, _port: int) -> list[str]:
    return ["8.8.8.8"]


def make_client(
    handler,
    *,
    resolver=public_resolver,
    allow_private_networks: bool = False,
) -> WebhookClient:
    return WebhookClient(
        timeout_seconds=1,
        allow_private_networks=allow_private_networks,
        resolver=resolver,
        transport=httpx.MockTransport(handler),
    )


def make_notification() -> WebhookNotification:
    return WebhookNotification(
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("42.50"),
        currency=Currency.RUB,
        processed_at=datetime.now(UTC),
        metadata={"order_id": "42"},
    )


async def test_webhook_client_sends_payload_and_stable_delivery_id() -> None:
    captured_request: httpx.Request | None = None

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_request
        captured_request = request
        return httpx.Response(204)

    notification = make_notification()
    client = make_client(handler)

    await client.send("https://client.example/webhook", notification)

    assert captured_request is not None
    assert captured_request.headers["X-Webhook-Id"] == str(notification.payment_id)
    assert json.loads(captured_request.content) == notification.model_dump(mode="json")


@pytest.mark.parametrize("status_code", [408, 425, 429, 500, 503])
async def test_webhook_client_retries_transient_response(
    status_code: int,
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code)

    client = make_client(handler)

    with pytest.raises(WebhookDeliveryError) as error:
        await client.send(
            "https://client.example/webhook",
            make_notification(),
        )

    assert isinstance(error.value.__cause__, httpx.HTTPStatusError)


@pytest.mark.parametrize("status_code", [300, 400, 401, 404, 422])
async def test_webhook_client_rejects_permanent_response(
    status_code: int,
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code)

    client = make_client(handler)

    with pytest.raises(PermanentWebhookDeliveryError) as error:
        await client.send(
            "https://client.example/webhook",
            make_notification(),
        )

    assert isinstance(error.value.__cause__, httpx.HTTPStatusError)


async def test_webhook_client_wraps_transport_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client = make_client(handler)

    with pytest.raises(WebhookDeliveryError) as error:
        await client.send(
            "https://client.example/webhook",
            make_notification(),
        )

    assert isinstance(error.value.__cause__, httpx.ConnectError)


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/webhook",
        "http://127.0.0.1/webhook",
        "http://10.0.0.1/webhook",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/webhook",
    ],
)
async def test_webhook_client_blocks_non_public_literal_addresses(url: str) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("Unsafe destination must not receive a request")

    client = make_client(handler)

    with pytest.raises(
        PermanentWebhookDeliveryError,
        match=r"not allowed|non-public",
    ):
        await client.send(url, make_notification())


async def test_webhook_client_blocks_hostname_resolving_to_private_address() -> None:
    async def private_resolver(_host: str, _port: int) -> list[str]:
        return ["192.168.1.10"]

    async def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("Unsafe destination must not receive a request")

    client = make_client(handler, resolver=private_resolver)

    with pytest.raises(PermanentWebhookDeliveryError, match="non-public"):
        await client.send("https://internal.example/webhook", make_notification())


async def test_webhook_client_treats_dns_failure_as_transient() -> None:
    async def failing_resolver(_host: str, _port: int) -> list[str]:
        raise socket.gaierror("temporary failure")

    async def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("Unresolved destination must not receive a request")

    client = make_client(handler, resolver=failing_resolver)

    with pytest.raises(WebhookDeliveryError, match="Could not resolve"):
        await client.send("https://unresolved.example/webhook", make_notification())


async def test_webhook_client_can_allow_private_networks_explicitly() -> None:
    captured_request: httpx.Request | None = None

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_request
        captured_request = request
        return httpx.Response(204)

    client = make_client(handler, allow_private_networks=True)

    await client.send("http://127.0.0.1/webhook", make_notification())

    assert captured_request is not None
