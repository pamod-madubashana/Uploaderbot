from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

from uploaderbot.config import Config
from uploaderbot.downloader import DownloadedFile
from uploaderbot.mp4 import VideoAttributes
from uploaderbot.store import SQLiteUploadStore
from uploaderbot.worker import UploadWorker


class UploadWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_notify_queue_changed_wakes_sleeping_worker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            config = build_config(base_dir)
            store = SQLiteUploadStore(config)
            worker = UploadWorker(object(), store, config)
            try:
                wait_task = asyncio.create_task(worker._wait_for_wake_or_timeout(5))
                await asyncio.sleep(0.05)
                worker.notify_queue_changed()
                await asyncio.wait_for(wait_task, timeout=0.5)
            finally:
                store.close()

    async def test_worker_removes_404_item_without_crashing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            config = build_config(base_dir)
            store = SQLiteUploadStore(config)
            try:
                store.enqueue_urls(["https://example.com/missing.mp4"])
                worker = UploadWorker(object(), store, config)

                request = httpx.Request("GET", "https://example.com/missing.mp4")
                response = httpx.Response(404, request=request)

                async def failing_download(*args, **kwargs):
                    raise httpx.HTTPStatusError("Not found", request=request, response=response)

                with patch("uploaderbot.worker.download_to_file", side_effect=failing_download):
                    with patch("uploaderbot.worker.IDLE_POLL_SECONDS", 0.01):
                        task = asyncio.create_task(worker.run())
                        for _ in range(50):
                            progress = store.get_batch_progress(1, 1)
                            if progress["total_count"] == 0:
                                break
                            await asyncio.sleep(0.01)
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task

                progress = store.get_batch_progress(1, 1)
                state = store.get_state()

                self.assertEqual(progress["total_count"], 0)
                self.assertEqual(progress["last_error"], "HTTP 404 for https://example.com/missing.mp4")
                self.assertNotEqual(state.get("status"), "crashed")
            finally:
                store.close()

    async def test_upload_copies_item_to_all_configured_chats(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            config = build_config(base_dir, chat_ids=[111, 222, 333])
            store = SQLiteUploadStore(config)
            bot = SimpleNamespace(
                send_video=AsyncMock(return_value=SimpleNamespace(message_id=9001)),
                send_photo=AsyncMock(),
                send_document=AsyncMock(),
                copy_message=AsyncMock(
                    side_effect=[
                        SimpleNamespace(message_id=9002),
                        SimpleNamespace(message_id=9003),
                    ]
                ),
                delete_message=AsyncMock(),
            )
            worker = UploadWorker(bot, store, config)
            downloaded_file = DownloadedFile(
                path=base_dir / "video.mp4",
                filename="video.mp4",
                size_bytes=1024,
            )
            video_attributes = VideoAttributes(
                duration_seconds=12,
                width=320,
                height=180,
                thumbnail_path=None,
                supports_streaming=True,
            )

            try:
                message, media_label = await worker._upload_downloaded_file(
                    downloaded_file,
                    "video",
                    "video.mp4",
                    video_attributes=video_attributes,
                )

                self.assertEqual(media_label, "video")
                self.assertEqual(message.message_id, 9001)
                bot.send_video.assert_awaited_once()
                self.assertEqual(bot.send_video.await_args.kwargs["chat_id"], 111)
                self.assertEqual(bot.copy_message.await_count, 2)
                self.assertEqual(bot.copy_message.await_args_list[0].kwargs["chat_id"], 222)
                self.assertEqual(bot.copy_message.await_args_list[1].kwargs["chat_id"], 333)
                bot.delete_message.assert_not_called()
            finally:
                store.close()


def build_config(base_dir: Path, *, chat_ids: list[int] | None = None) -> Config:
    return Config(
        token="token",
        database_uri=f"sqlite:///{base_dir / 'state.db'}",
        database_name="telegram_uploader",
        chat_ids=chat_ids or [123456],
        queue_file=base_dir / "unused.txt",
        download_dir=base_dir / "downloads",
        max_download_size_bytes=50 * 1024 * 1024,
        retry_delay_seconds=1,
        sqlite_db_file=base_dir / "fallback.db",
    )


if __name__ == "__main__":
    unittest.main()
