from unittest.mock import AsyncMock, Mock
from uuid import uuid4

from app.models.outbox import Outbox
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository


async def test_payment_repository_get_passes_lock_option(
    payment_factory,
) -> None:
    payment = payment_factory()
    session = Mock()
    session.get = AsyncMock(return_value=payment)
    repository = PaymentRepository(session)

    result = await repository.get(
        payment.payment_id,
        for_update=True,
    )

    assert result is payment
    session.get.assert_awaited_once_with(
        type(payment),
        payment.payment_id,
        with_for_update=True,
    )


async def test_payment_repository_queries_idempotency_key(
    payment_factory,
) -> None:
    payment = payment_factory()
    session = Mock()
    session.scalar = AsyncMock(return_value=payment)
    repository = PaymentRepository(session)

    result = await repository.get_by_idempotency_key("request-key")

    assert result is payment
    statement = session.scalar.await_args.args[0]
    assert "payments.idempotency_key" in str(statement)


async def test_payment_repository_adds_flushes_and_refreshes(
    payment_factory,
) -> None:
    payment = payment_factory()
    session = Mock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    repository = PaymentRepository(session)

    result = await repository.create(payment)

    assert result is payment
    session.add.assert_called_once_with(payment)
    session.flush.assert_awaited_once_with()
    session.refresh.assert_awaited_once_with(payment)


async def test_outbox_repository_adds_and_flushes_event() -> None:
    event = Outbox(
        outbox_id=uuid4(),
        event_type="payment.created",
        payload={"payment_id": str(uuid4())},
    )
    session = Mock()
    session.flush = AsyncMock()
    repository = OutboxRepository(session)

    result = await repository.create(event)

    assert result is event
    session.add.assert_called_once_with(event)
    session.flush.assert_awaited_once_with()
