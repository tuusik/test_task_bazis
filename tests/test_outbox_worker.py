import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import app.workers.outbox as worker_module


class SessionContextStub:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *_args: object) -> None:
        return None


async def test_wait_or_stop_returns_when_event_is_set() -> None:
    stop_event = asyncio.Event()
    stop_event.set()

    await worker_module.wait_or_stop(stop_event, delay=60)


async def test_wait_or_stop_returns_after_timeout() -> None:
    await worker_module.wait_or_stop(
        asyncio.Event(),
        delay=0,
    )


async def test_worker_publishes_batch_and_closes_resources(
    monkeypatch,
) -> None:
    stop_event = asyncio.Event()
    broker = SimpleNamespace(
        start=AsyncMock(),
        stop=AsyncMock(),
    )
    engine = SimpleNamespace(dispose=AsyncMock())
    publish_batch = AsyncMock()

    async def publish_and_stop(*, limit: int) -> int:
        stop_event.set()
        return 2

    publish_batch.side_effect = publish_and_stop
    monkeypatch.setattr(worker_module, "broker", broker)
    monkeypatch.setattr(worker_module, "engine", engine)
    monkeypatch.setattr(
        worker_module,
        "SessionLocal",
        lambda: SessionContextStub(),
    )
    monkeypatch.setattr(
        worker_module,
        "OutboxService",
        lambda *_args, **_kwargs: SimpleNamespace(publish_batch=publish_batch),
    )
    monkeypatch.setattr(
        worker_module,
        "settings",
        SimpleNamespace(
            outbox_poll_seconds=1.0,
            outbox_batch_size=50,
            outbox_claim_ttl_seconds=30,
            outbox_publish_timeout_seconds=10,
            outbox_retry_base_seconds=1,
            outbox_max_backoff_seconds=60,
        ),
    )
    monkeypatch.setattr(worker_module, "logger", Mock())

    await worker_module.run_outbox_worker(
        stop_event,
        register_signal_handlers=False,
    )

    broker.start.assert_awaited_once_with()
    publish_batch.assert_awaited_once_with(limit=50)
    broker.stop.assert_awaited_once_with()
    engine.dispose.assert_awaited_once_with()


async def test_worker_applies_backoff_after_publish_error(
    monkeypatch,
) -> None:
    stop_event = asyncio.Event()
    broker = SimpleNamespace(
        start=AsyncMock(),
        stop=AsyncMock(),
    )
    engine = SimpleNamespace(dispose=AsyncMock())
    publish_batch = AsyncMock(side_effect=RuntimeError("broker unavailable"))
    wait_or_stop = AsyncMock(side_effect=lambda *_args: stop_event.set())
    monkeypatch.setattr(worker_module, "broker", broker)
    monkeypatch.setattr(worker_module, "engine", engine)
    monkeypatch.setattr(
        worker_module,
        "SessionLocal",
        lambda: SessionContextStub(),
    )
    monkeypatch.setattr(
        worker_module,
        "OutboxService",
        lambda *_args, **_kwargs: SimpleNamespace(publish_batch=publish_batch),
    )
    monkeypatch.setattr(worker_module, "wait_or_stop", wait_or_stop)
    monkeypatch.setattr(
        worker_module,
        "settings",
        SimpleNamespace(
            outbox_poll_seconds=1.0,
            outbox_batch_size=50,
            outbox_claim_ttl_seconds=30,
            outbox_publish_timeout_seconds=10,
            outbox_retry_base_seconds=1,
            outbox_max_backoff_seconds=60,
        ),
    )
    monkeypatch.setattr(worker_module, "logger", Mock())

    await worker_module.run_outbox_worker(
        stop_event,
        register_signal_handlers=False,
    )

    wait_or_stop.assert_awaited_once_with(stop_event, 1.0)
    broker.stop.assert_awaited_once_with()
    engine.dispose.assert_awaited_once_with()
