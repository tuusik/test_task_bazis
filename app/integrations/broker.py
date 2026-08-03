from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import get_settings
from app.schemas.events import PaymentCreatedEvent

settings = get_settings()

broker = RabbitBroker(settings.rabbitmq_url)

payment_exchange = RabbitExchange(
    name=settings.payment_exchange,
    durable=True,
)

payment_dead_letter_exchange = RabbitExchange(
    name=settings.payment_dead_letter_exchange,
    durable=True,
)

payment_queue = RabbitQueue(
    name=settings.payment_queue,
    routing_key=settings.payment_queue,
    durable=True,
    arguments={
        "x-dead-letter-exchange": payment_dead_letter_exchange.name,
        "x-dead-letter-routing-key": (settings.payment_dead_letter_routing_key),
    },
)

payment_dead_letter_queue = RabbitQueue(
    name=settings.payment_dead_letter_queue,
    routing_key=settings.payment_dead_letter_routing_key,
    durable=True,
)

payment_created_publisher = broker.publisher(
    queue=payment_queue,
    exchange=payment_exchange,
    persist=True,
    schema=PaymentCreatedEvent,
)
