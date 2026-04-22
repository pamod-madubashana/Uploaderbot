from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from telegram.error import TelegramError

from .config import Config
from .downloader import DownloadTooLargeError, DownloadedFile, download_to_file
from .media import detect_media_type, short_name_from_url
from .mp4 import Mp4ProcessingError, VideoAttributes, prepare_video_file
from .store import UploadStore


logger = logging.getLogger("uploaderbot")
IDLE_POLL_SECONDS = 2
SKIP_REASON = "Removed by /skip"


def format_bytes(size_bytes: int) -> str:
    size = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size_bytes} B"


class UploadWorker:
    def __init__(self, bot, store: UploadStore, config: Config) -> None:
        self.bot = bot
        self.store = store
        self.config = config
        self._run_lock = asyncio.Lock()
        self._current_item: dict[str, object] | None = None
        self._current_item_task: asyncio.Task[tuple[object, str]] | None = None
        self._skip_requested_item_ids: set[str] = set()

    async def run(self) -> None:
        async with self._run_lock:
            try:
                await asyncio.to_thread(self.store.recover_pending_items)
                state = await asyncio.to_thread(self.store.refresh_state)
                logger.info("Upload worker started with %s queued items.", state["total_count"])

                while True:
                    next_item = await asyncio.to_thread(self.store.get_next_item)
                    if next_item is None:
                        await asyncio.to_thread(self.store.refresh_state)
                        await asyncio.sleep(IDLE_POLL_SECONDS)
                        continue

                    current_item = await asyncio.to_thread(self.store.mark_uploading, next_item["_id"])
                    if current_item is None:
                        await asyncio.sleep(1)
                        continue

                    await asyncio.to_thread(self.store.refresh_state, status="uploading", last_error=None)
                    logger.info(
                        "Preparing line %s as %s: %s",
                        current_item["line_number"],
                        detect_media_type(current_item["url"]),
                        short_name_from_url(current_item["url"]),
                    )

                    item_id = str(current_item["_id"])
                    item_task = asyncio.create_task(
                        self._send_item(current_item),
                        name=f"upload-item-{item_id}",
                    )
                    self._current_item = current_item
                    self._current_item_task = item_task

                    try:
                        message, media_type = await item_task
                    except asyncio.CancelledError:
                        if item_id in self._skip_requested_item_ids:
                            self._skip_requested_item_ids.discard(item_id)
                            await asyncio.to_thread(self.store.mark_removed, item_id, SKIP_REASON)
                            await asyncio.to_thread(self.store.refresh_state, last_error=SKIP_REASON)
                            logger.info("Removed current item after /skip: line %s", current_item["line_number"])
                            continue
                        raise
                    except DownloadTooLargeError as exc:
                        error_message = str(exc)
                        await asyncio.to_thread(self.store.mark_removed, item_id, error_message)
                        await asyncio.to_thread(self.store.refresh_state, last_error=error_message)
                        logger.info(
                            "Removed oversized file from queue on line %s: %s",
                            current_item["line_number"],
                            error_message,
                        )
                        continue
                    except TelegramError as exc:
                        error_message = str(exc)
                        await asyncio.to_thread(
                            self.store.mark_pending_after_error,
                            item_id,
                            error_message,
                        )
                        await asyncio.to_thread(
                            self.store.refresh_state,
                            status="waiting_retry",
                            last_error=error_message,
                        )
                        logger.warning(
                            "Upload failed for line %s, retrying in %s seconds: %s",
                            current_item["line_number"],
                            self.config.retry_delay_seconds,
                            error_message,
                        )
                        await asyncio.sleep(self.config.retry_delay_seconds)
                        continue
                    finally:
                        if self._current_item_task is item_task:
                            self._current_item_task = None
                        if self._current_item == current_item:
                            self._current_item = None
                        self._skip_requested_item_ids.discard(item_id)

                    await asyncio.to_thread(
                        self.store.mark_uploaded,
                        item_id,
                        getattr(message, "message_id", None),
                        media_type,
                    )
                    await asyncio.to_thread(self.store.refresh_state, last_error=None)
                    logger.info("Uploaded line %s successfully.", current_item["line_number"])
            except asyncio.CancelledError:
                logger.info("Upload worker stopped.")
                raise
            except Exception as exc:
                await asyncio.to_thread(self.store.refresh_state, status="crashed", last_error=str(exc))
                logger.exception("Upload worker crashed.")
                raise

    async def _send_item(self, item: dict[str, object]):
        total_count = (await asyncio.to_thread(self.store.get_state)).get("total_count", 0)
        caption = f"{item['line_number']}/{total_count} - {short_name_from_url(str(item['url']))}"

        media_type = detect_media_type(str(item["url"]))
        logger.info(
            "Downloading line %s from source URL: %s",
            item["line_number"],
            item["url"],
        )
        downloaded_file = await download_to_file(
            str(item["url"]),
            self.config.download_dir,
            max_size_bytes=self.config.max_download_size_bytes,
        )
        video_attributes = VideoAttributes()
        if media_type == "video":
            try:
                video_attributes = await asyncio.to_thread(prepare_video_file, downloaded_file.path)
            except Mp4ProcessingError as exc:
                logger.warning("Could not prepare MP4 metadata for %s: %s", downloaded_file.path, exc)
        logger.info(
            "Downloaded line %s to local file: %s (%s)",
            item["line_number"],
            downloaded_file.path,
            format_bytes(downloaded_file.size_bytes),
        )

        try:
            return await self._upload_downloaded_file(
                downloaded_file,
                media_type,
                caption,
                video_attributes=video_attributes,
            )
        finally:
            await self._delete_downloaded_file(downloaded_file.path)

    async def _upload_downloaded_file(
        self,
        downloaded_file: DownloadedFile,
        media_type: str,
        caption: str,
        *,
        video_attributes: VideoAttributes,
    ):
        logger.info(
            "Uploading local file to Telegram as %s: %s (%s)",
            media_type,
            downloaded_file.path,
            format_bytes(downloaded_file.size_bytes),
        )

        if media_type == "video":
            message = await self.bot.send_video(
                chat_id=self.config.chat_id,
                video=downloaded_file.path,
                filename=downloaded_file.filename,
                caption=caption,
                duration=video_attributes.duration_seconds,
                width=video_attributes.width,
                height=video_attributes.height,
                supports_streaming=video_attributes.supports_streaming,
                read_timeout=600,
                write_timeout=600,
                connect_timeout=60,
                pool_timeout=60,
            )
            logger.info("Telegram upload finished as video: %s", downloaded_file.path)
            return message, "video"

        if media_type == "photo":
            message = await self.bot.send_photo(
                chat_id=self.config.chat_id,
                photo=downloaded_file.path,
                caption=caption,
                read_timeout=600,
                write_timeout=600,
                connect_timeout=60,
                pool_timeout=60,
            )
            logger.info("Telegram upload finished as photo: %s", downloaded_file.path)
            return message, "photo"

        message = await self.bot.send_document(
            chat_id=self.config.chat_id,
            document=downloaded_file.path,
            filename=downloaded_file.filename,
            caption=caption,
            read_timeout=600,
            write_timeout=600,
            connect_timeout=60,
            pool_timeout=60,
        )
        logger.info("Telegram upload finished as document: %s", downloaded_file.path)
        return message, "document"

    async def _delete_downloaded_file(self, path: Path) -> None:
        logger.info("Deleting local file: %s", path)
        try:
            await asyncio.to_thread(path.unlink, True)
        except FileNotFoundError:
            logger.warning("Local file already missing during cleanup: %s", path)
            return

        logger.info("Deleted local file: %s", path)

    async def skip_current_item(self) -> dict[str, object] | None:
        current_item = self._current_item
        current_task = self._current_item_task

        if current_item is None or current_task is None or current_task.done():
            return None

        self._skip_requested_item_ids.add(str(current_item["_id"]))
        current_task.cancel()
        return current_item
