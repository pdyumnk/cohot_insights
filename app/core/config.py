"""Environment-based configuration. Every field maps to an upper-case env var."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Cohort Insights API"
    log_level: str = "INFO"

    # Storage
    mongo_url: str = "mongodb://localhost:27017"
    mongo_db: str = "cohort_insights"
    mongo_timeout_ms: int = Field(default=3000, ge=100)
    redis_url: str = "redis://localhost:6379/0"
    redis_timeout_s: float = Field(default=0.5, gt=0)

    # Per-user rate limit (documents concurrently in queued|processing|enriching)
    max_active_per_user: int = Field(default=3, ge=1)
    active_set_ttl_s: int = Field(default=3600, ge=60)

    # Content cache
    cache_ttl_s: int = Field(default=7 * 24 * 3600, ge=60)

    # Pipeline
    embedded_workers: bool = False  # run workers inside the API process (dev/tests)
    worker_concurrency: int = Field(default=3, ge=1)
    worker_poll_interval_s: float = Field(default=0.5, gt=0)
    stage_lease_s: float = Field(default=60, gt=0)
    stage_time_scale: float = Field(default=1.0, ge=0)  # multiplies simulated durations + backoff
    failure_rate: float = Field(default=0.1, ge=0, le=1)
    max_stage_attempts: int = Field(default=3, ge=1)
    retry_base_delay_s: float = Field(default=1.0, ge=0)


@lru_cache
def get_settings() -> Settings:
    return Settings()
