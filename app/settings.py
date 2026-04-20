from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "rcp-salary-audit"
    app_env: str = "dev"
    log_level: str = "INFO"

    host: str = "127.0.0.1"
    port: int = 8012

    session_secret: str = "change-me-in-env"

    database_url: str
    db_connect_timeout: int = 5

    target_salary_vnd: int = 5_600_000
    preview_max_rows: int = 20_000
    create_tables_on_startup: bool = False
    allow_local_ingest: bool = False
    default_excel_path: str | None = None

    @field_validator("database_url", mode="before")
    @classmethod
    def _normalize_database_url(cls, value: object) -> str:
        url = str(value or "").strip()
        if not url:
            return url

        # Prefer psycopg (v3) driver.
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://") :]
        if url.startswith("postgresql://"):
            url = "postgresql+psycopg://" + url[len("postgresql://") :]
        url = url.replace("postgresql+psycopg2://", "postgresql+psycopg://")
        return url


settings = Settings()  # type: ignore[call-arg]
