import pytest
from pydantic import ValidationError

from app.core.config import Settings


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("webhook_timeout_seconds", 0),
        ("webhook_claim_ttl_seconds", 0),
        ("webhook_claim_retry_seconds", 0),
        ("consumer_retry_attempts", 0),
        ("consumer_retry_base_seconds", -1),
        ("consumer_processing_time", (-1, 5)),
        ("consumer_processing_time", (5, 2)),
        ("consumer_process_chance", (0, 0)),
        ("consumer_process_chance", (90, -10)),
        ("outbox_poll_seconds", 0),
        ("outbox_batch_size", 0),
        ("outbox_claim_ttl_seconds", 0),
        ("outbox_publish_timeout_seconds", 0),
        ("outbox_retry_base_seconds", 0),
        ("outbox_max_backoff_seconds", 0),
    ],
)
def test_settings_reject_invalid_worker_values(
    field: str,
    value: object,
) -> None:
    values = {
        "api_key": "test-api-key",
        field: value,
    }

    with pytest.raises(ValidationError):
        Settings(_env_file=None, **values)


@pytest.mark.parametrize(
    "values",
    [
        {
            "webhook_timeout_seconds": 5,
            "webhook_claim_ttl_seconds": 5,
        },
        {
            "outbox_publish_timeout_seconds": 10,
            "outbox_claim_ttl_seconds": 10,
        },
    ],
)
def test_claim_ttl_must_outlive_network_timeout(
    values: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match="claim_ttl_seconds must be greater"):
        Settings(
            _env_file=None,
            api_key="test-api-key",
            **values,
        )
