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

                next_item = store.get_next_item()
                self.assertIsNotNone(next_item)
                assert next_item is not None
                uploading_item = store.mark_uploading(next_item["_id"])
                self.assertIsNotNone(uploading_item)
                assert uploading_item is not None
                store.mark_uploaded(uploading_item["_id"], 1001, "video")

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

                next_item = store.get_next_item()
                self.assertIsNotNone(next_item)
                assert next_item is not None
                current_item = store.mark_uploading(next_item["_id"])
                self.assertIsNotNone(current_item)
                assert current_item is not None

                store.mark_removed(current_item["_id"], "Removed by /skip")

                progress = store.get_batch_progress(1, 2)
                self.assertEqual(progress["total_count"], 1)
                self.assertEqual(progress["uploaded_count"], 0)
                self.assertEqual(progress["next_line_number"], 2)
                self.assertEqual(progress["last_error"], "Removed by /skip")
            finally:
                store.close()

    def test_enqueue_urls_inserts_new_items_before_existing_pending_items(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = build_config(Path(temp_dir), database_uri=f"sqlite:///{Path(temp_dir) / 'state.db'}")
            store = SQLiteUploadStore(config)
            try:
                store.enqueue_urls(
                    [
                        "https://example.com/1.mp4",
                        "https://example.com/2.mp4",
                        "https://example.com/3.mp4",
                    ]
                )
                current_item = store.get_next_item()
                self.assertIsNotNone(current_item)
                assert current_item is not None
                current_item = store.mark_uploading(current_item["_id"])
                self.assertIsNotNone(current_item)

                result = store.enqueue_urls(["https://example.com/new.mp4"])

                self.assertEqual(result["first_line_number"], 2)
                next_item = store.get_next_item()
                self.assertIsNotNone(next_item)
                assert next_item is not None
                self.assertEqual(next_item["url"], "https://example.com/new.mp4")

                original_second = store._fetchone(
                    "SELECT * FROM upload_items WHERE url = ?",
                    ("https://example.com/2.mp4",),
                )
                original_third = store._fetchone(
                    "SELECT * FROM upload_items WHERE url = ?",
                    ("https://example.com/3.mp4",),
                )
                self.assertIsNotNone(original_second)
                self.assertIsNotNone(original_third)
                assert original_second is not None
                assert original_third is not None
                self.assertEqual(original_second["line_number"], 3)
                self.assertEqual(original_third["line_number"], 4)
            finally:
                store.close()

    def test_batch_progress_reports_queued_when_other_item_is_uploading(self) -> None:
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
                next_item = store.get_next_item()
                self.assertIsNotNone(next_item)
                assert next_item is not None
                store.mark_uploading(next_item["_id"])

                progress = store.get_batch_progress(2, 2)

                self.assertEqual(progress["status"], "queued")
                self.assertEqual(progress["next_line_number"], 2)
            finally:
                store.close()

    def test_remove_active_items_clears_pending_and_uploading_items(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = build_config(Path(temp_dir), database_uri=f"sqlite:///{Path(temp_dir) / 'state.db'}")
            store = SQLiteUploadStore(config)
            try:
                store.enqueue_urls(
                    [
                        "https://example.com/1.mp4",
                        "https://example.com/2.mp4",
                        "https://example.com/3.mp4",
                    ]
                )
                first_item = store.get_next_item()
                self.assertIsNotNone(first_item)
                assert first_item is not None
                uploading_item = store.mark_uploading(first_item["_id"])
                self.assertIsNotNone(uploading_item)

                removed_count = store.remove_active_items("Cleared by /cancel")

                self.assertEqual(removed_count, 3)
                state = store.refresh_state()
                self.assertEqual(state["total_count"], 0)
                self.assertEqual(state["status"], "idle")
            finally:
                store.close()

    def test_progress_watches_persist_and_can_be_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = build_config(Path(temp_dir), database_uri=f"sqlite:///{Path(temp_dir) / 'state.db'}")
            store = SQLiteUploadStore(config)
            try:
                store.save_progress_watch(
                    chat_id=123,
                    message_id=456,
                    source_label="message",
                    first_line_number=10,
                    last_line_number=20,
                )

                watches = store.list_progress_watches()

                self.assertEqual(len(watches), 1)
                self.assertEqual(watches[0]["chat_id"], 123)
                self.assertEqual(watches[0]["message_id"], 456)
                self.assertEqual(watches[0]["first_line_number"], 10)
                self.assertEqual(watches[0]["last_line_number"], 20)

                store.delete_progress_watch(456)

                self.assertEqual(store.list_progress_watches(), [])
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
        chat_ids=[123456],
        queue_file=base_dir / "unused.txt",
        download_dir=base_dir / "downloads",
        max_download_size_bytes=50 * 1024 * 1024,
        retry_delay_seconds=1,
        sqlite_db_file=base_dir / "fallback.db",
    )


if __name__ == "__main__":
    unittest.main()
