import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Sequence

import httpx

from app.core.exceptions import (
    PermanentWebhookDeliveryError,
    WebhookDeliveryError,
)
from app.schemas.events import WebhookNotification

RETRYABLE_WEBHOOK_STATUS_CODES = {408, 425, 429}
HostResolver = Callable[[str, int], Awaitable[Sequence[str]]]


async def resolve_host(host: str, port: int) -> list[str]:
    address_info = await asyncio.to_thread(
        socket.getaddrinfo,
        host,
        port,
        type=socket.SOCK_STREAM,
    )
    return sorted({item[4][0] for item in address_info})


class WebhookClient:
    def __init__(
        self,
        timeout_seconds: float,
        *,
        allow_private_networks: bool = False,
        resolver: HostResolver | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.allow_private_networks = allow_private_networks
        self.resolver = resolver or resolve_host
        self.transport = transport

    async def send(self, url: str, notification: WebhookNotification) -> None:
        await self._validate_destination(url)

        try:
            async with httpx.AsyncClient(
                timeout=self.timeout_seconds,
                transport=self.transport,
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await client.post(
                    url,
                    json=notification.model_dump(mode="json"),
                    headers={"X-Webhook-Id": str(notification.payment_id)},
                )
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            error_class = (
                WebhookDeliveryError
                if status_code in RETRYABLE_WEBHOOK_STATUS_CODES or status_code >= 500
                else PermanentWebhookDeliveryError
            )
            raise error_class(
                f"Webhook delivery returned HTTP {status_code} for payment "
                f"{notification.payment_id}"
            ) from exc
        except httpx.RequestError as exc:
            raise WebhookDeliveryError(
                f"Webhook delivery failed for payment {notification.payment_id}"
            ) from exc

    async def _validate_destination(self, url: str) -> None:
        if self.allow_private_networks:
            return

        target = httpx.URL(url)
        if target.scheme not in {"http", "https"} or target.host is None:
            raise PermanentWebhookDeliveryError("Webhook URL must use HTTP or HTTPS")

        host = target.host.rstrip(".").lower()
        if host == "localhost" or host.endswith(".localhost"):
            raise PermanentWebhookDeliveryError(
                "Webhook destination localhost is not allowed"
            )

        port = target.port or (443 if target.scheme == "https" else 80)
        try:
            addresses = [str(ipaddress.ip_address(host.split("%", 1)[0]))]
        except ValueError:
            try:
                addresses = list(await self.resolver(host, port))
            except OSError as exc:
                raise WebhookDeliveryError(
                    f"Could not resolve webhook destination {host}"
                ) from exc

        if not addresses:
            raise WebhookDeliveryError(
                f"Webhook destination {host} resolved to no addresses"
            )

        for address in addresses:
            try:
                parsed_address = ipaddress.ip_address(address.split("%", 1)[0])
            except ValueError as exc:
                raise WebhookDeliveryError(
                    f"Resolver returned an invalid address for {host}"
                ) from exc

            if not parsed_address.is_global:
                raise PermanentWebhookDeliveryError(
                    f"Webhook destination {host} resolved to non-public address "
                    f"{parsed_address}"
                )
