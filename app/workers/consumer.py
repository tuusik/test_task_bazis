import asyncio
from typing import Any

from faststream import AckPolicy, FastStream, Logger
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError

from app.core.config import get_settings
from app.core.exceptions import (
    PermanentPaymentProcessingError,
    WebhookDeliveryInProgressError,
)
from app.db.session import SessionLocal
from app.integrations.broker import (
    broker,
    payment_dead_letter_exchange,
    payment_dead_letter_queue,
    payment_exchange,
    payment_queue,
)
from app.integrations.webhook import WebhookClient
from app.schemas.events import PaymentCreatedEvent
from app.services.payment_processor import PaymentProcessor
from app.services.payment_simulator import PaymentSimulator

RETRY_HEADER = "x-retry-count"

settings = get_settings()
simulator = PaymentSimulator(settings)
webhook_client = WebhookClient(
    settings.webhook_timeout_seconds,
    allow_private_networks=settings.webhook_allow_private_networks,
)
processor = PaymentProcessor(
    SessionLocal,
    simulator,
    webhook_client,
    webhook_claim_ttl_seconds=settings.webhook_claim_ttl_seconds,
)

app = FastStream(broker)


def get_attempt(message: RabbitMessage) -> int:
    raw_retry_count = (message.headers or {}).get(RETRY_HEADER, 0)

    try:
        retry_count = int(raw_retry_count)
    except (TypeError, ValueError):
        retry_count = 0

    return max(retry_count, 0) + 1


async def retry_or_dead_letter(
    payload: dict[str, Any],
    message: RabbitMessage,
    attempt: int,
    error: Exception,
    logger: Logger,
) -> None:
    if attempt >= settings.consumer_retry_attempts:
        logger.error(
            "Message exhausted %s attempts and is moving to DLQ: "
            "message_id=%s error=%s",
            attempt,
            message.message_id,
            error,
        )
        await message.reject(requeue=False)
        return

    delay = settings.consumer_retry_base_seconds * (2 ** (attempt - 1))
    logger.warning(
        "Payment processing failed on attempt %s; retrying in %.2fs: "
        "message_id=%s error=%s",
        attempt,
        delay,
        message.message_id,
        error,
    )
    await asyncio.sleep(delay)

    try:
        await broker.publish(
            payload,
            queue=payment_queue,
            exchange=payment_exchange,
            persist=True,
            mandatory=True,
            message_id=message.message_id,
            headers={RETRY_HEADER: attempt},
        )
    except Exception:
        logger.exception(
            "Could not publish retry; requeueing the original message: message_id=%s",
            message.message_id,
        )
        await message.nack(requeue=True)
        return

    await message.ack()


async def handle_payment_message(
    payload: dict[str, Any],
    message: RabbitMessage,
    logger: Logger,
) -> None:
    attempt = get_attempt(message)

    try:
        event = PaymentCreatedEvent.model_validate(payload)
    except ValidationError as exc:
        logger.error(
            "Invalid payment event is moving directly to DLQ: message_id=%s error=%s",
            message.message_id,
            exc,
        )
        await message.reject(requeue=False)
        return

    try:
        payment = await processor.process(event)
    except WebhookDeliveryInProgressError as exc:
        delay = min(
            exc.retry_after_seconds,
            settings.webhook_claim_retry_seconds,
        )
        logger.info(
            "Webhook delivery is claimed by another message; "
            "requeueing in %.2fs: payment_id=%s message_id=%s",
            delay,
            event.payment_id,
            message.message_id,
        )
        await asyncio.sleep(delay)
        await message.nack(requeue=True)
        return
    except PermanentPaymentProcessingError as exc:
        logger.error(
            "Permanent payment processing error is moving directly to DLQ: "
            "payment_id=%s message_id=%s error=%s",
            event.payment_id,
            message.message_id,
            exc,
        )
        await message.reject(requeue=False)
        return
    except Exception as exc:
        await retry_or_dead_letter(
            payload,
            message,
            attempt,
            exc,
            logger,
        )
        return

    logger.info(
        "Payment processed and webhook delivered: payment_id=%s status=%s",
        payment.payment_id,
        payment.status,
    )
    await message.ack()


@broker.subscriber(payment_queue, payment_exchange, ack_policy=AckPolicy.MANUAL)
async def handle_payment_created(
    payload: dict[str, Any], message: RabbitMessage, logger: Logger
) -> None:
    await handle_payment_message(payload, message, logger)


@app.after_startup
async def declare_dead_letter_topology() -> None:
    dead_letter_exchange = await broker.declare_exchange(payment_dead_letter_exchange)
    dead_letter_queue = await broker.declare_queue(payment_dead_letter_queue)
    await dead_letter_queue.bind(
        dead_letter_exchange, routing_key=settings.payment_dead_letter_routing_key
    )
