from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse


VIDEO_EXTENSIONS = {
    ".3gp",
    ".avi",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".webm",
}
PHOTO_EXTENSIONS = {
    ".bmp",
    ".gif",
    ".jpeg",
    ".jpg",
    ".png",
    ".webp",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def read_queue_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Queue file not found: {path}")

    urls: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls


def short_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = PurePosixPath(parsed.path).name
    return name or url


def detect_media_type(url: str) -> str:
    suffix = PurePosixPath(urlparse(url).path).suffix.lower()
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    if suffix in PHOTO_EXTENSIONS:
        return "photo"
    return "document"
