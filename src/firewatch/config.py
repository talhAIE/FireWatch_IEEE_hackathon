"""Validated configuration; error messages and representations omit secret values."""

import re
from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


def _is_placeholder(value: str) -> bool:
    return not value.strip() or value.upper().startswith(("REPLACE_", "CHANGE_ME"))


class Settings(BaseSettings):
    """Environment overrides .env; provider/chain secrets are conditional capabilities."""

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.firms"),
        env_file_encoding="utf-8",
        extra="forbid",
        hide_input_in_errors=True,
    )

    app_env: Literal["development", "test", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    database_url: SecretStr
    admin_api_token: SecretStr
    # POSTGRES_* describes provisioning; connections use DATABASE_URL exclusively.
    postgres_db: str = "firewatch"
    postgres_user: str = "firewatch"
    postgres_password: SecretStr | None = None
    firms_map_key: SecretStr | None = None
    public_reads_enabled: bool = True
    cors_origins: list[str] = Field(default_factory=list)
    regions_config: Path = Path("configs/regions.yaml")
    raw_data_dir: Path = Path("data/raw")
    source_artifact_dir: Path | None = None
    model_dir: Path = Path("models")
    replay_bundle_dir: Path = Path("data/replays")
    replays_config: Path = Path("configs/replays.yaml")
    max_request_body_bytes: int = Field(default=16384, ge=1024, le=1048576)
    request_body_timeout_seconds: int = Field(default=5, ge=1, le=30)
    max_job_queue_depth: int = Field(default=100, ge=1, le=10000)
    queue_lock_timeout_ms: int = Field(default=3000, ge=100, le=30000)
    max_refresh_window_hours: int = Field(default=48, ge=1, le=72)
    max_refresh_age_hours: int = Field(default=72, ge=1, le=168)
    max_replay_window_days: int = Field(default=31, ge=1, le=366)
    max_region_area_degrees2: float = Field(default=100, gt=0, le=1000)
    active_scorer: str = "baseline-v1"
    live_scorer: str | None = Field(
        default=None,
        min_length=1,
        description="Explicit live-only scorer; unset inherits ACTIVE_SCORER without fallback.",
    )
    refresh_interval_minutes: int = Field(default=15, ge=1, le=1440)
    worker_max_attempts: int = Field(default=5, ge=1, le=20)
    worker_lease_seconds: int = Field(default=120, ge=15, le=3600)
    worker_poll_seconds: float = Field(default=2, ge=0.1, le=60)
    worker_retry_seconds: int = Field(default=30, ge=1, le=3600)
    worker_health_max_lag_seconds: int = Field(default=600, ge=30, le=86400)
    detection_max_age_hours: float = Field(default=24, gt=0, le=168)
    features_config: Path = Path("configs/features-v1.yaml")
    weather_cache_grid_degrees: float = Field(default=0.1, gt=0, le=5)
    weather_max_age_hours: float = Field(default=3, gt=0, le=168)
    cluster_radius_km: float = Field(default=2, gt=0, le=100)
    snapshot_lookback_hours: int = Field(default=6, ge=1, le=168)
    label_horizon_hours: int = Field(default=24, ge=1, le=168)
    feature_schema_version: str = "features-v1"
    evidence_schema_version: str = "evidence-v1"
    chain_enabled: bool = False
    chain_rpc_url: SecretStr = SecretStr("http://127.0.0.1:8545")
    chain_id: int = Field(default=31337, gt=0)
    alert_registry_address: str = ""
    chain_private_key: SecretStr | None = None
    chain_network_kind: Literal["local", "testnet", "mainnet"] = "local"
    chain_local_rpc_hosts: list[str] = Field(
        default_factory=lambda: ["127.0.0.1", "localhost", "::1"]
    )
    chain_confirmations: int = Field(default=2, ge=1, le=256)
    chain_rpc_timeout_seconds: float = Field(default=3, ge=0.1, le=15)
    chain_poll_seconds: int = Field(default=5, ge=1, le=60)
    chain_recheck_seconds: int = Field(default=60, ge=1, le=3600)
    chain_max_attempts: int = Field(default=20, ge=1, le=100)
    chain_gas_limit: int = Field(default=100000, ge=50000, le=500000)
    chain_max_gas_price_wei: int = Field(default=200000000000, ge=1)
    public_api_hostname: str = ""
    db_pool_size: int = Field(default=5, ge=1, le=20)
    db_max_overflow: int = Field(default=5, ge=0, le=20)
    db_pool_timeout_seconds: int = Field(default=10, ge=1, le=60)
    db_connect_timeout_seconds: int = Field(default=5, ge=1, le=30)
    db_statement_timeout_ms: int = Field(default=30000, ge=100, le=300000)
    startup_timeout_seconds: float = Field(default=120, ge=1, le=600)
    startup_retry_seconds: float = Field(default=1, ge=0.1, le=30)

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        try:
            url = make_url(value.get_secret_value())
        except ArgumentError:
            raise ValueError("DATABASE_URL must be a valid PostgreSQL connection URL") from None
        if url.drivername != "postgresql+psycopg":
            raise ValueError("DATABASE_URL must use postgresql+psycopg")
        if not url.host or not url.database or not url.username:
            raise ValueError("DATABASE_URL requires host, database and username")
        if url.password is None or _is_placeholder(url.password):
            raise ValueError("DATABASE_URL requires a real password, not a placeholder")
        return value

    @field_validator("admin_api_token")
    @classmethod
    def validate_admin_token(cls, value: SecretStr) -> SecretStr:
        token = value.get_secret_value()
        if _is_placeholder(token) or not re.fullmatch(r"[A-Za-z0-9_-]{32,256}", token):
            raise ValueError(
                "ADMIN_API_TOKEN must be a generated URL-safe secret of 32-256 characters"
            )
        return value

    @field_validator("firms_map_key", "chain_private_key", mode="before")
    @classmethod
    def empty_optional_secret(cls, value: object) -> object:
        if isinstance(value, str) and _is_placeholder(value):
            return None
        return value

    @field_validator("cors_origins")
    @classmethod
    def validate_origins(cls, origins: list[str]) -> list[str]:
        for origin in origins:
            url = urlsplit(origin)
            if (
                url.scheme not in {"http", "https"}
                or not url.hostname
                or url.username
                or url.password
                or url.path not in {"", "/"}
                or url.query
                or url.fragment
                or "*" in origin
            ):
                raise ValueError("CORS_ORIGINS must contain explicit HTTP(S) origins")
        return [origin.rstrip("/") for origin in origins]

    @model_validator(mode="after")
    def validate_chain_configuration(self) -> "Settings":
        if self.app_env == "production" and "public_reads_enabled" not in self.model_fields_set:
            self.public_reads_enabled = False
        if self.chain_enabled:
            url = urlsplit(self.chain_rpc_url.get_secret_value())
            if url.scheme not in {"http", "https"} or not url.hostname:
                raise ValueError("Enabled chain requires a valid HTTP(S) CHAIN_RPC_URL")
            key = self.chain_private_key.get_secret_value() if self.chain_private_key else ""
            if key and not re.fullmatch(r"(?:0x)?[0-9a-fA-F]{64}", key):
                raise ValueError("CHAIN_PRIVATE_KEY must be 32-byte hexadecimal when supplied")
            if not re.fullmatch(r"0x[0-9a-fA-F]{40}", self.alert_registry_address):
                raise ValueError("Enabled chain requires a valid ALERT_REGISTRY_ADDRESS")
            if int(self.alert_registry_address[2:], 16) == 0:
                raise ValueError("Enabled chain requires a nonzero registry address")
            if (
                self.chain_network_kind == "local"
                and url.hostname not in self.chain_local_rpc_hosts
            ):
                raise ValueError("Local chain RPC must use an explicitly allowed local hostname")
        return self


class ConfigurationError(RuntimeError):
    """Safe configuration summary without environment values or credentials."""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as exc:
        fields = sorted(
            {
                ".".join(str(part) for part in item["loc"]) or "chain configuration"
                for item in exc.errors(include_input=False, include_context=False)
            }
        )
        raise ConfigurationError(
            "Invalid or missing configuration: "
            + ", ".join(fields)
            + ". Check .env against .env.example; required secrets cannot be placeholders."
        ) from None
