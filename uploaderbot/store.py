from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Protocol

from pymongo import ASCENDING, MongoClient, ReturnDocument, UpdateOne
from pymongo.errors import PyMongoError

from .config import Config
from .constants import KEEP_VALUE, STATE_DOCUMENT_ID
from .media import utc_now


logger = logging.getLogger("uploaderbot")


class UploadStore(Protocol):
    def close(self) -> None: ...

    def sync_queue(self, urls: list[str]) -> dict[str, Any]: ...

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

    def sync_queue(self, urls: list[str]) -> dict[str, Any]:
        logger.info("Syncing %s queued items to MongoDB state store.", len(urls))
        existing_items = {
            item["_id"]: item
            for item in self.items.find({}, {"url": 1, "status": 1})
        }
        logger.info("Fetched %s existing MongoDB queue records.", len(existing_items))

        active_ids: list[str] = []
        operations: list[UpdateOne] = []
        now = utc_now()
        total_urls = len(urls)

        for line_number, url in enumerate(urls, start=1):
            item_id = str(line_number)
            active_ids.append(item_id)
            existing = existing_items.get(item_id)

            if existing is None:
                operations.append(
                    UpdateOne(
                        {"_id": item_id},
                        {
                            "$set": {
                                "line_number": line_number,
                                "url": url,
                                "status": "pending",
                                "attempts": 0,
                                "message_id": None,
                                "media_type": None,
                                "last_error": None,
                                "updated_at": now,
                            },
                            "$setOnInsert": {"created_at": now},
                            "$unset": {"started_at": "", "completed_at": "", "removed_at": ""},
                        },
                        upsert=True,
                    )
                )
            else:
                update_fields: dict[str, Any] = {"line_number": line_number, "updated_at": now}
                update_operation: dict[str, Any] = {"$set": update_fields}

                if existing.get("url") != url or existing.get("status") == "removed":
                    update_fields.update(
                        {
                            "url": url,
                            "status": "pending",
                            "attempts": 0,
                            "message_id": None,
                            "media_type": None,
                            "last_error": None,
                        }
                    )
                    update_operation["$unset"] = {
                        "started_at": "",
                        "completed_at": "",
                        "removed_at": "",
                    }

                operations.append(
                    UpdateOne(
                        {"_id": item_id},
                        update_operation,
                    )
                )

            if line_number % 100 == 0 or line_number == total_urls:
                logger.info("Prepared MongoDB sync operations: %s/%s", line_number, total_urls)

        if operations:
            result = self.items.bulk_write(operations, ordered=False)
            logger.info(
                "MongoDB bulk sync applied: matched=%s modified=%s upserted=%s",
                result.matched_count,
                result.modified_count,
                len(result.upserted_ids),
            )

        if active_ids:
            self.items.update_many(
                {"_id": {"$nin": active_ids}, "status": {"$ne": "removed"}},
                {"$set": {"status": "removed", "removed_at": now, "updated_at": now}},
            )
        else:
            self.items.update_many(
                {"status": {"$ne": "removed"}},
                {"$set": {"status": "removed", "removed_at": now, "updated_at": now}},
            )

        self.items.update_many(
            {"status": "uploading"},
            {"$set": {"status": "pending", "updated_at": now}, "$unset": {"started_at": ""}},
        )

        logger.info("MongoDB queue sync finished.")

        return self.refresh_state(status="ready")

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

    def sync_queue(self, urls: list[str]) -> dict[str, Any]:
        logger.info("Syncing %s queued items to SQLite state store.", len(urls))
        now = utc_now().isoformat()
        active_ids: list[str] = []

        for line_number, url in enumerate(urls, start=1):
            item_id = str(line_number)
            active_ids.append(item_id)
            existing = self._fetchone(
                "SELECT url, status FROM upload_items WHERE id = ?",
                (item_id,),
            )

            if existing is None:
                self.connection.execute(
                    """
                    INSERT INTO upload_items (
                        id, line_number, url, status, attempts, message_id, media_type,
                        last_error, created_at, updated_at
                    ) VALUES (?, ?, ?, 'pending', 0, NULL, NULL, NULL, ?, ?)
                    """,
                    (item_id, line_number, url, now, now),
                )
                continue

            if existing.get("url") != url or existing.get("status") == "removed":
                self.connection.execute(
                    """
                    UPDATE upload_items
                    SET line_number = ?, url = ?, status = 'pending', attempts = 0,
                        message_id = NULL, media_type = NULL, last_error = NULL,
                        started_at = NULL, completed_at = NULL, removed_at = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (line_number, url, now, item_id),
                )
            else:
                self.connection.execute(
                    "UPDATE upload_items SET line_number = ?, updated_at = ? WHERE id = ?",
                    (line_number, now, item_id),
                )

        if active_ids:
            placeholders = ", ".join("?" for _ in active_ids)
            self.connection.execute(
                f"UPDATE upload_items SET status = 'removed', removed_at = ?, updated_at = ? WHERE status != 'removed' AND id NOT IN ({placeholders})",
                (now, now, *active_ids),
            )
        else:
            self.connection.execute(
                "UPDATE upload_items SET status = 'removed', removed_at = ?, updated_at = ? WHERE status != 'removed'",
                (now, now),
            )

        self.connection.execute(
            "UPDATE upload_items SET status = 'pending', started_at = NULL, updated_at = ? WHERE status = 'uploading'",
            (now,),
        )
        self.connection.commit()
        return self.refresh_state(status="ready")

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
