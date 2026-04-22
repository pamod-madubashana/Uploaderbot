from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from telegram.error import TelegramError

from .config import Config
from .downloader import DownloadedFile, download_to_file
from .media import detect_media_type, short_name_from_url
from .store import UploadStore


logger = logging.getLogger("uploaderbot")
IDLE_POLL_SECONDS = 2


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

                    try:
                        message, media_type = await self._send_item(current_item)
                    except asyncio.CancelledError:
                        raise
                    except TelegramError as exc:
                        error_message = str(exc)
                        await asyncio.to_thread(
                            self.store.mark_pending_after_error,
                            current_item["_id"],
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

                    await asyncio.to_thread(
                        self.store.mark_uploaded,
                        current_item["_id"],
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
        downloaded_file = await download_to_file(str(item["url"]), self.config.download_dir)
        logger.info(
            "Downloaded line %s to local file: %s (%s)",
            item["line_number"],
            downloaded_file.path,
            format_bytes(downloaded_file.size_bytes),
        )

        try:
            return await self._upload_downloaded_file(downloaded_file, media_type, caption)
        finally:
            await self._delete_downloaded_file(downloaded_file.path)

    async def _upload_downloaded_file(
        self,
        downloaded_file: DownloadedFile,
        media_type: str,
        caption: str,
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
                supports_streaming=True,
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
