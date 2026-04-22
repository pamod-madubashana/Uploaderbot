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


class DownloadTooLargeError(Exception):
    pass


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


def _parse_content_length(header_value: str | None) -> int | None:
    if not header_value:
        return None

    try:
        return int(header_value)
    except ValueError:
        return None


def _format_size(size_bytes: int) -> str:
    size = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size_bytes} B"


async def download_to_file(
    url: str,
    download_dir: Path,
    *,
    max_size_bytes: int,
) -> DownloadedFile:
    download_dir.mkdir(parents=True, exist_ok=True)
    filename = build_download_name(url)
    path = download_dir / filename

    logger.info("Starting download: %s -> %s", url, path)
    timeout = httpx.Timeout(connect=60.0, read=600.0, write=600.0, pool=60.0)
    size_bytes = 0

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                content_length = _parse_content_length(response.headers.get("content-length"))
                logger.info(
                    "Download response received: status=%s content_length=%s url=%s",
                    response.status_code,
                    response.headers.get("content-length", "unknown"),
                    url,
                )

                if content_length is not None and content_length > max_size_bytes:
                    raise DownloadTooLargeError(
                        f"Skipped {short_name_from_url(url)} because {_format_size(content_length)} exceeds the 50 MB limit."
                    )

                with path.open("wb") as target:
                    async for chunk in response.aiter_bytes(1024 * 1024):
                        if not chunk:
                            continue

                        size_bytes += len(chunk)
                        if size_bytes > max_size_bytes:
                            raise DownloadTooLargeError(
                                f"Skipped {short_name_from_url(url)} because it exceeds the 50 MB limit."
                            )
                        target.write(chunk)
    except BaseException:
        path.unlink(missing_ok=True)
        raise

    logger.info("Download completed: %s (%s bytes)", path, size_bytes)
    return DownloadedFile(path=path, filename=filename, size_bytes=size_bytes)
