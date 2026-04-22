from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from uploaderbot.config import Config
from uploaderbot.store import SQLiteUploadStore
from uploaderbot.worker import UploadWorker


class UploadWorkerTests(unittest.IsolatedAsyncioTestCase):
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


def build_config(base_dir: Path) -> Config:
    return Config(
        token="token",
        database_uri=f"sqlite:///{base_dir / 'state.db'}",
        database_name="telegram_uploader",
        chat_id=123456,
        queue_file=base_dir / "unused.txt",
        download_dir=base_dir / "downloads",
        max_download_size_bytes=50 * 1024 * 1024,
        retry_delay_seconds=1,
        sqlite_db_file=base_dir / "fallback.db",
    )


if __name__ == "__main__":
    unittest.main()
