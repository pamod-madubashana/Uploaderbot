from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass(slots=True)
class Config:
    token: str
    database_uri: str
    database_name: str
    chat_id: int
    queue_file: Path
    download_dir: Path
    retry_delay_seconds: int
    sqlite_db_file: Path

    @classmethod
    def from_env(cls, base_dir: Path) -> "Config":
        token = require_env("TOKEN")
        database_uri = require_env("DATABASE")
        chat_id = int(require_env("CHAT_ID"))
        database_name = os.getenv("DATABASE_NAME", "telegram_uploader")
        queue_file = base_dir / os.getenv("QUEUE_FILE", "vvv.txt")
        download_dir = base_dir / os.getenv("DOWNLOAD_DIR", "downloads")
        retry_delay_seconds = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
        sqlite_db_file = base_dir / os.getenv("SQLITE_DB_FILE", "upload_state.db")

        if retry_delay_seconds < 1:
            raise RuntimeError("RETRY_DELAY_SECONDS must be at least 1")

        return cls(
            token=token,
            database_uri=database_uri,
            database_name=database_name,
            chat_id=chat_id,
            queue_file=queue_file,
            download_dir=download_dir,
            retry_delay_seconds=retry_delay_seconds,
            sqlite_db_file=sqlite_db_file,
        )
