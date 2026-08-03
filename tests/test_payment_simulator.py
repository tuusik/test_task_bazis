from unittest.mock import AsyncMock, Mock

from app.domain.enums import PaymentStatus
from app.services.payment_simulator import PaymentSimulator


async def test_simulator_waits_and_returns_weighted_status(
    test_settings,
    monkeypatch,
) -> None:
    sleep = AsyncMock()
    uniform = Mock(return_value=3.5)
    choices = Mock(return_value=[PaymentStatus.FAILED])
    monkeypatch.setattr("app.services.payment_simulator.asyncio.sleep", sleep)
    monkeypatch.setattr("app.services.payment_simulator.random.uniform", uniform)
    monkeypatch.setattr("app.services.payment_simulator.random.choices", choices)
    simulator = PaymentSimulator(test_settings)

    result = await simulator.simulate()

    assert result is PaymentStatus.FAILED
    uniform.assert_called_once_with(2, 5)
    sleep.assert_awaited_once_with(3.5)
    choices.assert_called_once_with(
        population=[PaymentStatus.SUCCEEDED, PaymentStatus.FAILED],
        weights=(90, 10),
        k=1,
    )
