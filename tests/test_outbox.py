from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

from sqlalchemy.dialects import postgresql

from app.models.outbox import Outbox
from app.repositories.outbox import OutboxRepository
from app.schemas.events import PaymentCreatedEvent
from app.services.outbox import OutboxService


class ScalarResultStub:
    def __init__(self, values: list[Outbox]) -> None:
        self.values = values

    def all(self) -> list[Outbox]:
        return self.values


def make_outbox_event(*, event_type: str = "payment.created") -> Outbox:
    return Outbox(
        outbox_id=uuid4(),
        event_type=event_type,
        payload={"payment_id": str(uuid4())},
        created_at=datetime.now(UTC),
        next_attempt_at=datetime.now(UTC),
        publish_attempts=0,
    )


def make_service(session_stub, publisher) -> OutboxService:
    return OutboxService(
        session_stub,  # type: ignore[arg-type]
        publisher,
        claim_ttl_seconds=30,
        publish_timeout_seconds=10,
        retry_base_seconds=1,
        max_backoff_seconds=60,
    )


async def test_outbox_repository_claims_available_unlocked_events() -> None:
    event = make_outbox_event()
    session = Mock()
    session.scalars = AsyncMock(return_value=ScalarResultStub([event]))
    repository = OutboxRepository(session)
    now = datetime.now(UTC)
    stale_before = now - timedelta(seconds=30)

    result = await repository.claim_unpublished_batch(
        limit=25,
        now=now,
        stale_before=stale_before,
    )

    assert result == [event]
    assert event.claimed_at == now
    statement = session.scalars.await_args.args[0]
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "published_at IS NULL" in sql
    assert "next_attempt_at <=" in sql
    assert "claimed_at IS NULL" in sql
    assert "ORDER BY outbox.next_attempt_at, outbox.created_at" in sql
    assert "LIMIT 25" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql


def test_mark_as_published_clears_claim_and_error() -> None:
    event = make_outbox_event()
    event.claimed_at = datetime.now(UTC)
    event.last_error = "old error"
    published_at = datetime.now(UTC)

    OutboxRepository.mark_as_published(event, now=published_at)

    assert event.published_at == published_at
    assert event.claimed_at is None
    assert event.last_error is None
    assert event.publish_attempts == 1


def test_mark_as_failed_schedules_retry_and_releases_claim() -> None:
    event = make_outbox_event()
    event.claimed_at = datetime.now(UTC)
    next_attempt_at = datetime.now(UTC) + timedelta(seconds=1)

    OutboxRepository.mark_as_failed(
        event,
        error="broker unavailable",
        next_attempt_at=next_attempt_at,
    )

    assert event.claimed_at is None
    assert event.last_error == "broker unavailable"
    assert event.next_attempt_at == next_attempt_at
    assert event.publish_attempts == 1


async def test_outbox_service_publishes_outside_claim_transaction(
    session_stub,
) -> None:
    event = make_outbox_event()
    publisher = Mock(publish=AsyncMock())
    repository = Mock()
    repository.claim_unpublished_batch = AsyncMock(return_value=[event])
    repository.get = AsyncMock(return_value=event)
    repository.mark_as_published = Mock()
    repository.mark_as_failed = Mock()
    service = make_service(session_stub, publisher)
    service.repository = repository

    published_count = await service.publish_batch(limit=1)

    assert published_count == 1
    assert len(session_stub.transactions) == 2
    assert session_stub.transactions[0].exited_with is None
    assert session_stub.transactions[1].exited_with is None
    published_event = publisher.publish.await_args.args[0]
    assert published_event == PaymentCreatedEvent.model_validate(event.payload)
    assert publisher.publish.await_args.kwargs == {
        "message_id": str(event.outbox_id),
        "timeout": 10,
    }
    repository.mark_as_published.assert_called_once_with(event)
    repository.mark_as_failed.assert_not_called()


async def test_failed_outbox_event_does_not_block_next_event(
    session_stub,
) -> None:
    failed_event = make_outbox_event()
    successful_event = make_outbox_event()
    publisher = Mock(
        publish=AsyncMock(
            side_effect=[
                RuntimeError("broker unavailable"),
                None,
            ]
        )
    )
    repository = Mock()
    repository.claim_unpublished_batch = AsyncMock(
        side_effect=[[failed_event], [successful_event], []]
    )
    repository.get = AsyncMock(side_effect=[failed_event, successful_event])
    repository.mark_as_published = Mock()
    repository.mark_as_failed = Mock()
    service = make_service(session_stub, publisher)
    service.repository = repository

    published_count = await service.publish_batch(limit=10)

    assert published_count == 1
    assert publisher.publish.await_count == 2
    repository.mark_as_failed.assert_called_once()
    failed_call = repository.mark_as_failed.call_args
    assert failed_call.args[0] is failed_event
    assert "RuntimeError: broker unavailable" == failed_call.kwargs["error"]
    assert failed_call.kwargs["next_attempt_at"] > datetime.now(UTC)
    repository.mark_as_published.assert_called_once_with(successful_event)


async def test_unknown_event_type_is_recorded_as_failure(
    session_stub,
) -> None:
    event = make_outbox_event(event_type="unknown.event")
    publisher = Mock(publish=AsyncMock())
    repository = Mock()
    repository.claim_unpublished_batch = AsyncMock(side_effect=[[event], []])
    repository.get = AsyncMock(return_value=event)
    repository.mark_as_published = Mock()
    repository.mark_as_failed = Mock()
    service = make_service(session_stub, publisher)
    service.repository = repository

    published_count = await service.publish_batch(limit=10)

    assert published_count == 0
    publisher.publish.assert_not_awaited()
    repository.mark_as_failed.assert_called_once()
    assert (
        "Unsupported outbox event type"
        in (repository.mark_as_failed.call_args.kwargs["error"])
    )


async def test_outbox_claims_each_event_only_when_it_is_ready_to_publish(
    session_stub,
) -> None:
    first_event = make_outbox_event()
    second_event = make_outbox_event()
    publisher = Mock(publish=AsyncMock())
    repository = Mock()
    repository.claim_unpublished_batch = AsyncMock(
        side_effect=[[first_event], [second_event]]
    )
    repository.get = AsyncMock(side_effect=[first_event, second_event])
    repository.mark_as_published = Mock()
    repository.mark_as_failed = Mock()
    service = make_service(session_stub, publisher)
    service.repository = repository

    published_count = await service.publish_batch(limit=2)

    assert published_count == 2
    assert repository.claim_unpublished_batch.await_count == 2
    assert all(
        call.kwargs["limit"] == 1
        for call in repository.claim_unpublished_batch.await_args_list
    )
    assert publisher.publish.await_count == 2
