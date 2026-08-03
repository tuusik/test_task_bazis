import os
from uuid import uuid4

import aio_pika
import pytest
from aio_pika import DeliveryMode, ExchangeType, Message

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION_TESTS") != "1",
        reason="Set RUN_INTEGRATION_TESTS=1 to run integration tests",
    ),
]


async def test_rejected_message_is_routed_to_real_dead_letter_queue() -> None:
    rabbitmq_url = os.getenv(
        "TEST_RABBITMQ_URL",
        "amqp://payments:payments@localhost:5672/",
    )
    suffix = uuid4().hex
    exchange_name = f"integration.payments.{suffix}"
    dead_letter_exchange_name = f"integration.payments.dlx.{suffix}"
    queue_name = f"integration.payments.new.{suffix}"
    dead_letter_queue_name = f"integration.payments.dlq.{suffix}"
    routing_key = "payments.new"
    dead_letter_routing_key = "payments.failed"

    connection = await aio_pika.connect_robust(rabbitmq_url)
    channel = await connection.channel(publisher_confirms=True)
    exchange = await channel.declare_exchange(
        exchange_name,
        ExchangeType.DIRECT,
        durable=True,
        auto_delete=False,
    )
    dead_letter_exchange = await channel.declare_exchange(
        dead_letter_exchange_name,
        ExchangeType.DIRECT,
        durable=True,
        auto_delete=False,
    )
    queue = await channel.declare_queue(
        queue_name,
        durable=True,
        auto_delete=False,
        arguments={
            "x-dead-letter-exchange": dead_letter_exchange_name,
            "x-dead-letter-routing-key": dead_letter_routing_key,
        },
    )
    dead_letter_queue = await channel.declare_queue(
        dead_letter_queue_name,
        durable=True,
        auto_delete=False,
    )

    try:
        await queue.bind(exchange, routing_key)
        await dead_letter_queue.bind(
            dead_letter_exchange,
            dead_letter_routing_key,
        )
        body = b'{"payment_id":"00000000-0000-0000-0000-000000000001"}'
        await exchange.publish(
            Message(body, delivery_mode=DeliveryMode.PERSISTENT),
            routing_key,
            mandatory=True,
        )

        incoming = await queue.get(timeout=5)
        assert incoming is not None
        await incoming.reject(requeue=False)

        dead_lettered = await dead_letter_queue.get(timeout=5)
        assert dead_lettered is not None
        assert dead_lettered.body == body
        assert dead_lettered.headers["x-death"][0]["count"] == 1
        await dead_lettered.ack()
    finally:
        await queue.delete(if_unused=False, if_empty=False)
        await dead_letter_queue.delete(if_unused=False, if_empty=False)
        await exchange.delete(if_unused=False)
        await dead_letter_exchange.delete(if_unused=False)
        await connection.close()
