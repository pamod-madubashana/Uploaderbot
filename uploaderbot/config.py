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


def parse_chat_ids(value: str) -> list[int]:
    chat_ids: list[int] = []
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        try:
            chat_ids.append(int(part))
        except ValueError as exc:
            raise RuntimeError(f"Invalid chat id in CHAT_IDs: {part}") from exc

    if not chat_ids:
        raise RuntimeError("CHAT_IDs must include at least one chat id")
    return chat_ids


@dataclass(slots=True)
class Config:
    token: str
    database_uri: str
    database_name: str
    chat_ids: list[int]
    queue_file: Path
    download_dir: Path
    max_download_size_bytes: int
    retry_delay_seconds: int
    sqlite_db_file: Path

    @classmethod
    def from_env(cls, base_dir: Path) -> "Config":
        token = require_env("TOKEN")
        database_uri = require_env("DATABASE")
        raw_chat_ids = os.getenv("CHAT_IDs") or os.getenv("CHAT_IDS") or os.getenv("CHAT_ID")
        if not raw_chat_ids:
            raise RuntimeError("Missing required environment variable: CHAT_IDs")
        chat_ids = parse_chat_ids(raw_chat_ids)
        database_name = os.getenv("DATABASE_NAME", "telegram_uploader")
        queue_file = base_dir / os.getenv("QUEUE_FILE", "vvv.txt")
        download_dir = base_dir / os.getenv("DOWNLOAD_DIR", "downloads")
        max_download_size_mb = int(os.getenv("MAX_DOWNLOAD_SIZE_MB", "50"))
        retry_delay_seconds = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
        sqlite_db_file = base_dir / os.getenv("SQLITE_DB_FILE", "upload_state.db")

        if max_download_size_mb < 1:
            raise RuntimeError("MAX_DOWNLOAD_SIZE_MB must be at least 1")
        if retry_delay_seconds < 1:
            raise RuntimeError("RETRY_DELAY_SECONDS must be at least 1")

        return cls(
            token=token,
            database_uri=database_uri,
            database_name=database_name,
            chat_ids=chat_ids,
            queue_file=queue_file,
            download_dir=download_dir,
            max_download_size_bytes=max_download_size_mb * 1024 * 1024,
            retry_delay_seconds=retry_delay_seconds,
            sqlite_db_file=sqlite_db_file,
        )
