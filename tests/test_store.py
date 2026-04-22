from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pymongo.errors import PyMongoError

from uploaderbot.config import Config
from uploaderbot.store import SQLiteUploadStore, create_store


class StoreTests(unittest.TestCase):
    def test_sqlite_store_tracks_batch_progress(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = build_config(Path(temp_dir), database_uri=f"sqlite:///{Path(temp_dir) / 'state.db'}")
            store = SQLiteUploadStore(config)
            try:
                result = store.enqueue_urls(
                    [
                        "https://example.com/1.mp4",
                        "https://example.com/2.mp4",
                        "https://example.com/3.mp4",
                    ]
                )

                self.assertEqual(result["first_line_number"], 1)
                self.assertEqual(result["last_line_number"], 3)

                progress = store.get_batch_progress(1, 3)
                self.assertEqual(progress["total_count"], 3)
                self.assertEqual(progress["uploaded_count"], 0)
                self.assertEqual(progress["next_line_number"], 1)

                uploading_item = store.mark_uploading("1")
                self.assertIsNotNone(uploading_item)
                store.mark_uploaded("1", 1001, "video")

                progress = store.get_batch_progress(1, 3)
                self.assertEqual(progress["uploaded_count"], 1)
                self.assertEqual(progress["next_line_number"], 2)
            finally:
                store.close()

    def test_mark_removed_excludes_item_from_batch_progress(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = build_config(Path(temp_dir), database_uri=f"sqlite:///{Path(temp_dir) / 'state.db'}")
            store = SQLiteUploadStore(config)
            try:
                store.enqueue_urls(
                    [
                        "https://example.com/1.mp4",
                        "https://example.com/2.mp4",
                    ]
                )

                current_item = store.mark_uploading("1")
                self.assertIsNotNone(current_item)

                store.mark_removed("1", "Removed by /skip")

                progress = store.get_batch_progress(1, 2)
                self.assertEqual(progress["total_count"], 1)
                self.assertEqual(progress["uploaded_count"], 0)
                self.assertEqual(progress["next_line_number"], 2)
                self.assertEqual(progress["last_error"], "Removed by /skip")
            finally:
                store.close()

    def test_create_store_uses_sqlite_uri(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = build_config(Path(temp_dir), database_uri=f"sqlite:///{Path(temp_dir) / 'state.db'}")
            store = create_store(config)
            try:
                self.assertIsInstance(store, SQLiteUploadStore)
            finally:
                store.close()

    def test_create_store_falls_back_when_mongo_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = build_config(Path(temp_dir), database_uri="mongodb://example.invalid")
            with patch("uploaderbot.store.MongoUploadStore", side_effect=PyMongoError("boom")):
                store = create_store(config)

            try:
                self.assertIsInstance(store, SQLiteUploadStore)
            finally:
                store.close()


def build_config(base_dir: Path, *, database_uri: str) -> Config:
    return Config(
        token="token",
        database_uri=database_uri,
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
