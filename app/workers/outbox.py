import asyncio
import logging
import signal

from app.core.config import get_settings
from app.db.session import SessionLocal, engine
from app.integrations.broker import broker, payment_created_publisher
from app.services.outbox import OutboxService

logger = logging.getLogger(__name__)
settings = get_settings()


async def wait_or_stop(stop_event: asyncio.Event, delay: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay)
    except TimeoutError:
        pass


async def run_outbox_worker(
    stop_event: asyncio.Event | None = None, *, register_signal_handlers: bool = True
) -> None:
    stop_event = stop_event or asyncio.Event()

    if register_signal_handlers:
        loop = asyncio.get_running_loop()
        for signal_value in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signal_value, stop_event.set)

    retry_delay = settings.outbox_poll_seconds
    broker_started = False

    try:
        await broker.start()
        broker_started = True
        logger.info("Outbox worker started")

        while not stop_event.is_set():
            try:
                async with SessionLocal() as session:
                    service = OutboxService(
                        session,
                        payment_created_publisher,
                        claim_ttl_seconds=settings.outbox_claim_ttl_seconds,
                        publish_timeout_seconds=(
                            settings.outbox_publish_timeout_seconds
                        ),
                        retry_base_seconds=settings.outbox_retry_base_seconds,
                        max_backoff_seconds=(settings.outbox_max_backoff_seconds),
                    )
                    published_count = await service.publish_batch(
                        limit=settings.outbox_batch_size,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Failed to publish outbox batch")
                await wait_or_stop(stop_event, retry_delay)
                retry_delay = min(
                    retry_delay * 2,
                    settings.outbox_max_backoff_seconds,
                )
                continue

            retry_delay = settings.outbox_poll_seconds

            if published_count > 0:
                logger.info("Published %s outbox events", published_count)
            else:
                await wait_or_stop(stop_event, settings.outbox_poll_seconds)
    finally:
        if broker_started:
            await broker.stop()

        await engine.dispose()
        logger.info("Outbox worker stopped")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=("%(asctime)s %(levelname)s %(name)s: %(message)s"),
    )
    asyncio.run(run_outbox_worker())


if __name__ == "__main__":
    main()
