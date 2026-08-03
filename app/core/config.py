from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

NonNegativeInt = Annotated[int, Field(ge=0)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "test_task_bazis"
    app_env: Literal["local", "dev", "test", "prod"] = "local"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    api_prefix: str = "/api/v1"
    debug: bool = False

    api_key: SecretStr = Field(min_length=1)

    database_url: str = (
        "postgresql+asyncpg://study:study@localhost:5432/test_task_bazis"
    )

    sql_echo: bool = False

    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    webhook_timeout_seconds: float = Field(default=5, gt=0)
    webhook_claim_ttl_seconds: float = Field(default=30, gt=0)
    webhook_claim_retry_seconds: float = Field(default=0.5, gt=0)
    webhook_allow_private_networks: bool = False

    payment_queue: str = "payments.new"
    payment_exchange: str = "payments"
    payment_dead_letter_exchange: str = "payments.dlx"
    payment_dead_letter_queue: str = "payments.dlq"
    payment_dead_letter_routing_key: str = "payments.failed"

    consumer_processing_time: tuple[NonNegativeInt, NonNegativeInt] = (2, 5)
    consumer_process_chance: tuple[NonNegativeInt, NonNegativeInt] = (90, 10)
    consumer_retry_attempts: int = Field(default=3, ge=1)
    consumer_retry_base_seconds: float = Field(default=1, gt=0)

    outbox_poll_seconds: float = Field(default=1, gt=0)
    outbox_batch_size: int = Field(default=50, ge=1)
    outbox_claim_ttl_seconds: float = Field(default=30, gt=0)
    outbox_publish_timeout_seconds: float = Field(default=10, gt=0)
    outbox_retry_base_seconds: float = Field(default=1, gt=0)
    outbox_max_backoff_seconds: int = Field(default=60, gt=0)

    @model_validator(mode="after")
    def validate_worker_settings(self) -> "Settings":
        processing_min, processing_max = self.consumer_processing_time
        if processing_min > processing_max:
            raise ValueError("consumer_processing_time minimum must not exceed maximum")

        success_weight, failure_weight = self.consumer_process_chance
        if success_weight + failure_weight <= 0:
            raise ValueError("consumer_process_chance must contain a positive weight")

        if self.webhook_claim_ttl_seconds <= self.webhook_timeout_seconds:
            raise ValueError(
                "webhook_claim_ttl_seconds must be greater than webhook_timeout_seconds"
            )

        if self.outbox_claim_ttl_seconds <= self.outbox_publish_timeout_seconds:
            raise ValueError(
                "outbox_claim_ttl_seconds must be greater than "
                "outbox_publish_timeout_seconds"
            )

        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
