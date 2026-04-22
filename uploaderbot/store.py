from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from pymongo import ASCENDING, MongoClient, ReturnDocument, UpdateOne
from pymongo.errors import PyMongoError

from .config import Config
from .constants import KEEP_VALUE, STATE_DOCUMENT_ID
from .media import utc_now


logger = logging.getLogger("uploaderbot")
LINE_SHIFT_OFFSET = 1_000_000_000


class UploadStore(Protocol):
    def close(self) -> None: ...

    def enqueue_urls(self, urls: list[str]) -> dict[str, Any]: ...

    def get_batch_progress(self, first_line_number: int, last_line_number: int) -> dict[str, Any]: ...

    def recover_pending_items(self) -> None: ...

    def mark_removed(self, item_id: str, reason: str) -> None: ...

    def remove_active_items(self, reason: str) -> int: ...

    def get_next_item(self) -> dict[str, Any] | None: ...

    def mark_uploading(self, item_id: str) -> dict[str, Any] | None: ...

    def mark_uploaded(self, item_id: str, message_id: int | None, media_type: str) -> None: ...

    def mark_pending_after_error(self, item_id: str, error_message: str) -> None: ...

    def get_state(self) -> dict[str, Any]: ...

    def refresh_state(
        self,
        *,
        status: str | None = None,
        last_error: str | None | object = KEEP_VALUE,
    ) -> dict[str, Any]: ...


class MongoUploadStore:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.client = MongoClient(config.database_uri)
        self.client.admin.command("ping")
        self.db = self.client[config.database_name]
        self.items = self.db["upload_items"]
        self.state = self.db["upload_state"]
        self._ensure_indexes()

    def close(self) -> None:
        self.client.close()

    def _ensure_indexes(self) -> None:
        self.items.create_index([("status", ASCENDING), ("line_number", ASCENDING)])
        self.items.create_index([("line_number", ASCENDING)], unique=True)

    def enqueue_urls(self, urls: list[str]) -> dict[str, Any]:
        if not urls:
            return {
                "added_count": 0,
                "first_line_number": None,
                "last_line_number": None,
                "state": self.refresh_state(),
            }

        logger.info("Queueing %s items into MongoDB state store.", len(urls))
        now = utc_now()
        starting_line_number = self._get_insert_position()
        self._shift_items_for_insert(starting_line_number, len(urls), now)
        operations: list[UpdateOne] = []

        for offset, url in enumerate(urls):
            line_number = starting_line_number + offset
            operations.append(
                UpdateOne(
                    {"_id": self._build_item_id(line_number)},
                    {
                        "$set": {
                            "line_number": line_number,
                            "url": url,
                            "status": "pending",
                            "attempts": 0,
                            "message_id": None,
                            "media_type": None,
                            "last_error": None,
                            "created_at": now,
                            "updated_at": now,
                        }
                    },
                    upsert=True,
                )
            )

        if operations:
            result = self.items.bulk_write(operations, ordered=False)
            upserted_ids = result.upserted_ids if result.upserted_ids is not None else {}
            logger.info(
                "MongoDB queue insert applied: matched=%s modified=%s upserted=%s",
                result.matched_count,
                result.modified_count,
                len(upserted_ids),
            )

        logger.info("MongoDB queue insert finished.")

        return {
            "added_count": len(urls),
            "first_line_number": starting_line_number,
            "last_line_number": starting_line_number + len(urls) - 1,
            "state": self.refresh_state(last_error=None),
        }

    def get_batch_progress(self, first_line_number: int, last_line_number: int) -> dict[str, Any]:
        active_line_filter = {
            "line_number": {"$gte": first_line_number, "$lte": last_line_number},
            "status": {"$ne": "removed"},
        }
        line_filter = {"line_number": {"$gte": first_line_number, "$lte": last_line_number}}
        total_count = self.items.count_documents(active_line_filter)
        uploaded_count = self.items.count_documents({**active_line_filter, "status": "uploaded"})
        current_item = self.items.find_one({**active_line_filter, "status": "uploading"}, sort=[("line_number", ASCENDING)])
        next_item = self.items.find_one({**active_line_filter, "status": "pending"}, sort=[("line_number", ASCENDING)])
        error_item = self.items.find_one(
            {**line_filter, "last_error": {"$ne": None}},
            sort=[("updated_at", -1), ("line_number", -1)],
        )
        any_uploading_item = self.items.find_one({"status": "uploading"}, projection={"_id": 1})
        status = "completed"
        if total_count == 0:
            status = "idle"
        elif current_item is not None:
            status = "uploading"
        elif next_item is not None:
            status = "queued" if any_uploading_item is not None else "ready"

        return {
            "first_line_number": first_line_number,
            "last_line_number": last_line_number,
            "total_count": total_count,
            "uploaded_count": uploaded_count,
            "remaining_count": max(total_count - uploaded_count, 0),
            "current_line_number": current_item.get("line_number") if current_item else None,
            "current_url": current_item.get("url") if current_item else None,
            "next_line_number": next_item.get("line_number") if next_item else None,
            "next_url": next_item.get("url") if next_item else None,
            "last_error": error_item.get("last_error") if error_item else None,
            "status": status,
        }

    def recover_pending_items(self) -> None:
        now = utc_now()
        self.items.update_many(
            {"status": "uploading"},
            {"$set": {"status": "pending", "updated_at": now}, "$unset": {"started_at": ""}},
        )

    def mark_removed(self, item_id: str, reason: str) -> None:
        now = utc_now()
        self.items.update_one(
            {"_id": item_id},
            {
                "$set": {
                    "status": "removed",
                    "last_error": reason,
                    "removed_at": now,
                    "updated_at": now,
                },
                "$unset": {"started_at": ""},
            },
        )

    def remove_active_items(self, reason: str) -> int:
        now = utc_now()
        result = self.items.update_many(
            {"status": {"$nin": ["uploaded", "removed"]}},
            {
                "$set": {
                    "status": "removed",
                    "last_error": reason,
                    "removed_at": now,
                    "updated_at": now,
                },
                "$unset": {"started_at": ""},
            },
        )
        return int(result.modified_count)

    def _get_highest_line_number(self) -> int:
        item = self.items.find_one(sort=[("line_number", -1)], projection={"line_number": 1})
        if item is None:
            return 0
        return int(item.get("line_number", 0))

    def _get_insert_position(self) -> int:
        current_item = self.items.find_one(
            {"status": "uploading"},
            sort=[("line_number", ASCENDING)],
            projection={"line_number": 1},
        )
        if current_item is not None:
            return int(current_item["line_number"]) + 1

        next_item = self.items.find_one(
            {"status": "pending"},
            sort=[("line_number", ASCENDING)],
            projection={"line_number": 1},
        )
        if next_item is not None:
            return int(next_item["line_number"])

        return self._get_highest_line_number() + 1

    def _build_item_id(self, line_number: int) -> str:
        return f"{line_number}-{uuid4().hex}"

    def _shift_items_for_insert(self, insert_position: int, count: int, now) -> None:
        affected_items = list(
            self.items.find(
                {"status": {"$ne": "removed"}, "line_number": {"$gte": insert_position}},
                projection={"line_number": 1},
                sort=[("line_number", -1)],
            )
        )
        if not affected_items:
            return

        temp_operations = [
            UpdateOne(
                {"_id": item["_id"]},
                {"$set": {"line_number": int(item["line_number"]) + LINE_SHIFT_OFFSET, "updated_at": now}},
            )
            for item in affected_items
        ]
        self.items.bulk_write(temp_operations, ordered=True)

        final_operations = [
            UpdateOne(
                {"_id": item["_id"]},
                {"$set": {"line_number": int(item["line_number"]) + count, "updated_at": now}},
            )
            for item in affected_items
        ]
        self.items.bulk_write(final_operations, ordered=True)

    def get_next_item(self) -> dict[str, Any] | None:
        return self.items.find_one({"status": "pending"}, sort=[("line_number", ASCENDING)])

    def mark_uploading(self, item_id: str) -> dict[str, Any] | None:
        now = utc_now()
        return self.items.find_one_and_update(
            {"_id": item_id, "status": "pending"},
            {
                "$set": {"status": "uploading", "started_at": now, "updated_at": now},
                "$inc": {"attempts": 1},
            },
            return_document=ReturnDocument.AFTER,
        )

    def mark_uploaded(self, item_id: str, message_id: int | None, media_type: str) -> None:
        now = utc_now()
        self.items.update_one(
            {"_id": item_id},
            {
                "$set": {
                    "status": "uploaded",
                    "message_id": message_id,
                    "media_type": media_type,
                    "last_error": None,
                    "completed_at": now,
                    "updated_at": now,
                },
                "$unset": {"started_at": ""},
            },
        )

    def mark_pending_after_error(self, item_id: str, error_message: str) -> None:
        now = utc_now()
        self.items.update_one(
            {"_id": item_id},
            {
                "$set": {
                    "status": "pending",
                    "last_error": error_message,
                    "updated_at": now,
                },
                "$unset": {"started_at": ""},
            },
        )

    def get_state(self) -> dict[str, Any]:
        state = self.state.find_one({"_id": STATE_DOCUMENT_ID})
        if state is None:
            return self.refresh_state(status="idle", last_error=None)
        return state

    def refresh_state(
        self,
        *,
        status: str | None = None,
        last_error: str | None | object = KEEP_VALUE,
    ) -> dict[str, Any]:
        active_filter = {"status": {"$ne": "removed"}}
        total_count = self.items.count_documents(active_filter)
        uploaded_count = self.items.count_documents({"status": "uploaded"})
        current_item = self.items.find_one({"status": "uploading"}, sort=[("line_number", ASCENDING)])
        next_item = self.items.find_one({"status": "pending"}, sort=[("line_number", ASCENDING)])
        last_uploaded_item = self.items.find_one({"status": "uploaded"}, sort=[("line_number", -1)])
        previous_state = self.state.find_one({"_id": STATE_DOCUMENT_ID}) or {}

        effective_status = status
        if effective_status is None:
            if total_count == 0:
                effective_status = "idle"
            elif current_item is not None:
                effective_status = "uploading"
            elif next_item is not None:
                effective_status = "ready"
            else:
                effective_status = "completed"

        resolved_last_error = previous_state.get("last_error")
        if last_error is not KEEP_VALUE:
            resolved_last_error = last_error

        document = {
            "_id": STATE_DOCUMENT_ID,
            "queue_file": self.config.queue_file.name,
            "backend": "mongo",
            "status": effective_status,
            "total_count": total_count,
            "uploaded_count": uploaded_count,
            "remaining_count": max(total_count - uploaded_count, 0),
            "current_line_number": current_item.get("line_number") if current_item else None,
            "current_url": current_item.get("url") if current_item else None,
            "next_line_number": next_item.get("line_number") if next_item else None,
            "next_url": next_item.get("url") if next_item else None,
            "last_uploaded_line_number": last_uploaded_item.get("line_number") if last_uploaded_item else None,
            "last_uploaded_url": last_uploaded_item.get("url") if last_uploaded_item else None,
            "last_error": resolved_last_error,
            "updated_at": utc_now(),
        }

        if effective_status == "completed":
            document["finished_at"] = utc_now()

        self.state.update_one({"_id": STATE_DOCUMENT_ID}, {"$set": document}, upsert=True)
        return self.state.find_one({"_id": STATE_DOCUMENT_ID}) or document


class SQLiteUploadStore:
    def __init__(self, config: Config, db_file: Path | None = None) -> None:
        self.config = config
        self.db_file = db_file or config.sqlite_db_file
        self.connection = sqlite3.connect(self.db_file, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self._ensure_schema()

    def close(self) -> None:
        self.connection.close()

    def _ensure_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS upload_items (
                id TEXT PRIMARY KEY,
                line_number INTEGER NOT NULL UNIQUE,
                url TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                message_id INTEGER,
                media_type TEXT,
                last_error TEXT,
                created_at TEXT,
                updated_at TEXT,
                started_at TEXT,
                completed_at TEXT,
                removed_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_upload_items_status_line
            ON upload_items(status, line_number);

            CREATE TABLE IF NOT EXISTS upload_state (
                id TEXT PRIMARY KEY,
                queue_file TEXT,
                status TEXT,
                total_count INTEGER,
                uploaded_count INTEGER,
                remaining_count INTEGER,
                current_line_number INTEGER,
                current_url TEXT,
                next_line_number INTEGER,
                next_url TEXT,
                last_uploaded_line_number INTEGER,
                last_uploaded_url TEXT,
                last_error TEXT,
                updated_at TEXT,
                finished_at TEXT,
                backend TEXT
            );
            """
        )
        self.connection.commit()

    def _fetchone(self, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        row = self.connection.execute(query, params).fetchone()
        if row is None:
            return None

        data = dict(row)
        if "id" in data and "_id" not in data:
            data["_id"] = data["id"]
        return data

    def enqueue_urls(self, urls: list[str]) -> dict[str, Any]:
        if not urls:
            return {
                "added_count": 0,
                "first_line_number": None,
                "last_line_number": None,
                "state": self.refresh_state(),
            }

        logger.info("Queueing %s items into SQLite state store.", len(urls))
        now = utc_now().isoformat()
        starting_line_number = self._get_insert_position()
        self._shift_items_for_insert(starting_line_number, len(urls), now)

        for offset, url in enumerate(urls):
            line_number = starting_line_number + offset
            item_id = self._build_item_id(line_number)
            self.connection.execute(
                """
                INSERT INTO upload_items (
                    id, line_number, url, status, attempts, message_id, media_type,
                    last_error, created_at, updated_at
                ) VALUES (?, ?, ?, 'pending', 0, NULL, NULL, NULL, ?, ?)
                """,
                (item_id, line_number, url, now, now),
            )

        self.connection.commit()
        return {
            "added_count": len(urls),
            "first_line_number": starting_line_number,
            "last_line_number": starting_line_number + len(urls) - 1,
            "state": self.refresh_state(last_error=None),
        }

    def get_batch_progress(self, first_line_number: int, last_line_number: int) -> dict[str, Any]:
        params = (first_line_number, last_line_number)
        total_count = self.connection.execute(
            "SELECT COUNT(*) FROM upload_items WHERE status != 'removed' AND line_number BETWEEN ? AND ?",
            params,
        ).fetchone()[0]
        uploaded_count = self.connection.execute(
            "SELECT COUNT(*) FROM upload_items WHERE status = 'uploaded' AND line_number BETWEEN ? AND ?",
            params,
        ).fetchone()[0]
        current_item = self._fetchone(
            "SELECT * FROM upload_items WHERE status = 'uploading' AND line_number BETWEEN ? AND ? ORDER BY line_number ASC LIMIT 1",
            params,
        )
        next_item = self._fetchone(
            "SELECT * FROM upload_items WHERE status = 'pending' AND line_number BETWEEN ? AND ? ORDER BY line_number ASC LIMIT 1",
            params,
        )
        error_item = self._fetchone(
            "SELECT * FROM upload_items WHERE last_error IS NOT NULL AND line_number BETWEEN ? AND ? ORDER BY updated_at DESC, line_number DESC LIMIT 1",
            params,
        )
        any_uploading_item = self._fetchone(
            "SELECT id FROM upload_items WHERE status = 'uploading' ORDER BY line_number ASC LIMIT 1"
        )
        status = "completed"
        if total_count == 0:
            status = "idle"
        elif current_item is not None:
            status = "uploading"
        elif next_item is not None:
            status = "queued" if any_uploading_item is not None else "ready"

        return {
            "first_line_number": first_line_number,
            "last_line_number": last_line_number,
            "total_count": total_count,
            "uploaded_count": uploaded_count,
            "remaining_count": max(total_count - uploaded_count, 0),
            "current_line_number": current_item.get("line_number") if current_item else None,
            "current_url": current_item.get("url") if current_item else None,
            "next_line_number": next_item.get("line_number") if next_item else None,
            "next_url": next_item.get("url") if next_item else None,
            "last_error": error_item.get("last_error") if error_item else None,
            "status": status,
        }

    def recover_pending_items(self) -> None:
        now = utc_now().isoformat()
        self.connection.execute(
            "UPDATE upload_items SET status = 'pending', started_at = NULL, updated_at = ? WHERE status = 'uploading'",
            (now,),
        )
        self.connection.commit()

    def mark_removed(self, item_id: str, reason: str) -> None:
        now = utc_now().isoformat()
        self.connection.execute(
            """
            UPDATE upload_items
            SET status = 'removed', last_error = ?, removed_at = ?, started_at = NULL, updated_at = ?
            WHERE id = ?
            """,
            (reason, now, now, item_id),
        )
        self.connection.commit()

    def remove_active_items(self, reason: str) -> int:
        now = utc_now().isoformat()
        cursor = self.connection.execute(
            """
            UPDATE upload_items
            SET status = 'removed', last_error = ?, removed_at = ?, started_at = NULL, updated_at = ?
            WHERE status NOT IN ('uploaded', 'removed')
            """,
            (reason, now, now),
        )
        self.connection.commit()
        return int(cursor.rowcount)

    def _get_highest_line_number(self) -> int:
        row = self.connection.execute("SELECT COALESCE(MAX(line_number), 0) FROM upload_items").fetchone()
        return int(row[0]) if row is not None else 0

    def _get_insert_position(self) -> int:
        current_item = self._fetchone(
            "SELECT line_number FROM upload_items WHERE status = 'uploading' ORDER BY line_number ASC LIMIT 1"
        )
        if current_item is not None:
            return int(current_item["line_number"]) + 1

        next_item = self._fetchone(
            "SELECT line_number FROM upload_items WHERE status = 'pending' ORDER BY line_number ASC LIMIT 1"
        )
        if next_item is not None:
            return int(next_item["line_number"])

        return self._get_highest_line_number() + 1

    def _build_item_id(self, line_number: int) -> str:
        return f"{line_number}-{uuid4().hex}"

    def _shift_items_for_insert(self, insert_position: int, count: int, now: str) -> None:
        self.connection.execute(
            """
            UPDATE upload_items
            SET line_number = line_number + ?, updated_at = ?
            WHERE status != 'removed' AND line_number >= ?
            """,
            (LINE_SHIFT_OFFSET, now, insert_position),
        )
        self.connection.execute(
            """
            UPDATE upload_items
            SET line_number = line_number - ? + ?, updated_at = ?
            WHERE status != 'removed' AND line_number >= ?
            """,
            (LINE_SHIFT_OFFSET, count, now, insert_position + LINE_SHIFT_OFFSET),
        )

    def get_next_item(self) -> dict[str, Any] | None:
        return self._fetchone(
            "SELECT * FROM upload_items WHERE status = 'pending' ORDER BY line_number ASC LIMIT 1"
        )

    def mark_uploading(self, item_id: str) -> dict[str, Any] | None:
        now = utc_now().isoformat()
        cursor = self.connection.execute(
            """
            UPDATE upload_items
            SET status = 'uploading', started_at = ?, updated_at = ?, attempts = attempts + 1
            WHERE id = ? AND status = 'pending'
            """,
            (now, now, item_id),
        )
        self.connection.commit()
        if cursor.rowcount == 0:
            return None
        return self._fetchone("SELECT * FROM upload_items WHERE id = ?", (item_id,))

    def mark_uploaded(self, item_id: str, message_id: int | None, media_type: str) -> None:
        now = utc_now().isoformat()
        self.connection.execute(
            """
            UPDATE upload_items
            SET status = 'uploaded', message_id = ?, media_type = ?, last_error = NULL,
                completed_at = ?, started_at = NULL, updated_at = ?
            WHERE id = ?
            """,
            (message_id, media_type, now, now, item_id),
        )
        self.connection.commit()

    def mark_pending_after_error(self, item_id: str, error_message: str) -> None:
        now = utc_now().isoformat()
        self.connection.execute(
            """
            UPDATE upload_items
            SET status = 'pending', last_error = ?, started_at = NULL, updated_at = ?
            WHERE id = ?
            """,
            (error_message, now, item_id),
        )
        self.connection.commit()

    def get_state(self) -> dict[str, Any]:
        state = self._fetchone("SELECT * FROM upload_state WHERE id = ?", (STATE_DOCUMENT_ID,))
        if state is None:
            return self.refresh_state(status="idle", last_error=None)
        return state

    def refresh_state(
        self,
        *,
        status: str | None = None,
        last_error: str | None | object = KEEP_VALUE,
    ) -> dict[str, Any]:
        total_count = self.connection.execute(
            "SELECT COUNT(*) FROM upload_items WHERE status != 'removed'"
        ).fetchone()[0]
        uploaded_count = self.connection.execute(
            "SELECT COUNT(*) FROM upload_items WHERE status = 'uploaded'"
        ).fetchone()[0]
        current_item = self._fetchone(
            "SELECT * FROM upload_items WHERE status = 'uploading' ORDER BY line_number ASC LIMIT 1"
        )
        next_item = self._fetchone(
            "SELECT * FROM upload_items WHERE status = 'pending' ORDER BY line_number ASC LIMIT 1"
        )
        last_uploaded_item = self._fetchone(
            "SELECT * FROM upload_items WHERE status = 'uploaded' ORDER BY line_number DESC LIMIT 1"
        )
        previous_state = self._fetchone("SELECT * FROM upload_state WHERE id = ?", (STATE_DOCUMENT_ID,)) or {}

        effective_status = status
        if effective_status is None:
            if total_count == 0:
                effective_status = "idle"
            elif current_item is not None:
                effective_status = "uploading"
            elif next_item is not None:
                effective_status = "ready"
            else:
                effective_status = "completed"

        resolved_last_error = previous_state.get("last_error")
        if last_error is not KEEP_VALUE:
            resolved_last_error = last_error

        updated_at = utc_now().isoformat()
        finished_at = utc_now().isoformat() if effective_status == "completed" else None

        self.connection.execute(
            """
            INSERT INTO upload_state (
                id, queue_file, status, total_count, uploaded_count, remaining_count,
                current_line_number, current_url, next_line_number, next_url,
                last_uploaded_line_number, last_uploaded_url, last_error,
                updated_at, finished_at, backend
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                queue_file = excluded.queue_file,
                status = excluded.status,
                total_count = excluded.total_count,
                uploaded_count = excluded.uploaded_count,
                remaining_count = excluded.remaining_count,
                current_line_number = excluded.current_line_number,
                current_url = excluded.current_url,
                next_line_number = excluded.next_line_number,
                next_url = excluded.next_url,
                last_uploaded_line_number = excluded.last_uploaded_line_number,
                last_uploaded_url = excluded.last_uploaded_url,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at,
                finished_at = excluded.finished_at,
                backend = excluded.backend
            """,
            (
                STATE_DOCUMENT_ID,
                self.config.queue_file.name,
                effective_status,
                total_count,
                uploaded_count,
                max(total_count - uploaded_count, 0),
                current_item.get("line_number") if current_item else None,
                current_item.get("url") if current_item else None,
                next_item.get("line_number") if next_item else None,
                next_item.get("url") if next_item else None,
                last_uploaded_item.get("line_number") if last_uploaded_item else None,
                last_uploaded_item.get("url") if last_uploaded_item else None,
                resolved_last_error,
                updated_at,
                finished_at,
                "sqlite",
            ),
        )
        self.connection.commit()
        return self._fetchone("SELECT * FROM upload_state WHERE id = ?", (STATE_DOCUMENT_ID,)) or {}


def create_store(config: Config) -> UploadStore:
    if config.database_uri.startswith("sqlite:///"):
        sqlite_path = Path(config.database_uri.removeprefix("sqlite:///"))
        return SQLiteUploadStore(config, sqlite_path)

    try:
        logger.info("Connecting to MongoDB for upload state.")
        return MongoUploadStore(config)
    except PyMongoError as exc:
        logger.warning(
            "MongoDB connection failed, using SQLite fallback at %s: %s",
            config.sqlite_db_file,
            exc,
        )
        return SQLiteUploadStore(config)
