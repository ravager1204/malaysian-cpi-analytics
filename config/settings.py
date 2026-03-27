"""Centralized project settings."""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class DatabaseSettings:
    host: str = "localhost"
    port: int = 5432
    name: str = "cpi_analytics"
    user: str = "postgres"
    password: str = "postgres"

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.name}"
        )


@dataclass(frozen=True)
class AwsSettings:
    access_key_id: str | None
    secret_access_key: str | None
    region: str = "ap-southeast-1"
    bucket_name: str | None = None
    enable_upload: bool = False


@dataclass(frozen=True)
class ProjectSettings:
    project_root: Path
    raw_data_dir: Path
    processed_data_dir: Path
    outputs_dir: Path
    logs_dir: Path
    environment: str
    project_name: str
    database: DatabaseSettings
    aws: AwsSettings

    def ensure_runtime_dirs(self) -> None:
        for path in (
            self.raw_data_dir,
            self.processed_data_dir,
            self.outputs_dir,
            self.logs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> ProjectSettings:
    database = DatabaseSettings(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        name=os.getenv("DB_NAME", "cpi_analytics"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )
    aws = AwsSettings(
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region=os.getenv("AWS_REGION", "ap-southeast-1"),
        bucket_name=os.getenv("S3_BUCKET_NAME"),
        enable_upload=os.getenv("ENABLE_S3_UPLOAD", "false").lower() == "true",
    )
    return ProjectSettings(
        project_root=PROJECT_ROOT,
        raw_data_dir=PROJECT_ROOT / "data" / "raw",
        processed_data_dir=PROJECT_ROOT / "data" / "processed",
        outputs_dir=PROJECT_ROOT / "data" / "outputs",
        logs_dir=PROJECT_ROOT / "logs",
        environment=os.getenv("ENVIRONMENT", "development"),
        project_name=os.getenv("PROJECT_NAME", "malaysian-cpi-analytics"),
        database=database,
        aws=aws,
    )
