from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import httpx

from .media import short_name_from_url


FILENAME_SANITIZER = re.compile(r"[^A-Za-z0-9._-]+")
logger = logging.getLogger("uploaderbot")


@dataclass(slots=True)
class DownloadedFile:
    path: Path
    filename: str
    size_bytes: int


def build_download_name(url: str) -> str:
    raw_name = short_name_from_url(url)
    safe_name = FILENAME_SANITIZER.sub("_", raw_name).strip("._")
    if not safe_name:
        safe_name = "file"
    return f"{uuid4().hex}_{safe_name}"


async def download_to_file(url: str, download_dir: Path) -> DownloadedFile:
    download_dir.mkdir(parents=True, exist_ok=True)
    filename = build_download_name(url)
    path = download_dir / filename

    logger.info("Starting download: %s -> %s", url, path)
    timeout = httpx.Timeout(connect=60.0, read=600.0, write=600.0, pool=60.0)
    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            logger.info(
                "Download response received: status=%s content_length=%s url=%s",
                response.status_code,
                response.headers.get("content-length", "unknown"),
                url,
            )
            with path.open("wb") as target:
                async for chunk in response.aiter_bytes(1024 * 1024):
                    if chunk:
                        target.write(chunk)

    size_bytes = path.stat().st_size
    logger.info("Download completed: %s (%s bytes)", path, size_bytes)
    return DownloadedFile(path=path, filename=filename, size_bytes=size_bytes)
