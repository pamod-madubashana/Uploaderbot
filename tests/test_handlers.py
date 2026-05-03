from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast

from uploaderbot.handlers import _format_progress_message, _replace_chat_progress_watch


class FakeStore:
    def __init__(self, watches: list[dict[str, int]] | None = None) -> None:
        self._watches = list(watches or [])

    def list_progress_watches(self) -> list[dict[str, int]]:
        return list(self._watches)

    def delete_progress_watch(self, message_id: int) -> None:
        self._watches = [watch for watch in self._watches if watch["message_id"] != message_id]


class FakeBot:
    def __init__(self) -> None:
        self.deleted_messages: list[tuple[int, int]] = []

    async def delete_message(self, *, chat_id: int, message_id: int) -> None:
        self.deleted_messages.append((chat_id, message_id))


class ReplaceProgressWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_replace_chat_progress_watch_removes_previous_message_in_chat(self) -> None:
        store = FakeStore(
            [
                {"chat_id": 10, "message_id": 101},
                {"chat_id": 20, "message_id": 202},
            ]
        )
        bot = FakeBot()
        finished = asyncio.Future[None]()
        finished.set_result(None)
        application = SimpleNamespace(
            bot=bot,
            bot_data={
                "store": store,
                "progress_tasks": {
                    101: finished,
                },
            },
        )

        await _replace_chat_progress_watch(cast(Any, application), chat_id=10)

        self.assertEqual(store.list_progress_watches(), [{"chat_id": 20, "message_id": 202}])
        self.assertEqual(bot.deleted_messages, [(10, 101)])

    async def test_replace_chat_progress_watch_keeps_current_message(self) -> None:
        store = FakeStore(
            [
                {"chat_id": 10, "message_id": 101},
                {"chat_id": 10, "message_id": 102},
            ]
        )
        bot = FakeBot()
        application = SimpleNamespace(
            bot=bot,
            bot_data={
                "store": store,
                "progress_tasks": {},
            },
        )

        await _replace_chat_progress_watch(cast(Any, application), chat_id=10, keep_message_id=102)

        self.assertEqual(store.list_progress_watches(), [{"chat_id": 10, "message_id": 102}])
        self.assertEqual(bot.deleted_messages, [(10, 101)])


class HandlerFormattingTests(unittest.TestCase):
    def test_progress_message_uses_total_queue_counts(self) -> None:
        text = _format_progress_message(
            source_label="message",
            queue_state={
                "status": "uploading",
                "uploaded_count": 152,
                "total_count": 253,
                "current_line_number": 521,
                "current_url": "https://example.com/current.mp4",
                "next_line_number": 522,
                "next_url": "https://example.com/next.mp4",
                "last_error": None,
            },
        )

        self.assertIn("Progress: [", text)
        self.assertIn("152/253", text)
        self.assertIn("Current: 521 - current.mp4", text)
        self.assertIn("Next: 522 - next.mp4", text)

    def test_progress_message_uses_supplied_updated_timestamp(self) -> None:
        text = _format_progress_message(
            source_label="message",
            queue_state={
                "status": "uploading",
                "uploaded_count": 1,
                "total_count": 2,
                "current_line_number": None,
                "current_url": None,
                "next_line_number": None,
                "next_url": None,
                "last_error": None,
            },
            updated_at=datetime(2026, 5, 3, 12, 34, 56, tzinfo=timezone.utc),
        )

        self.assertIn("Updated: 12:34:56 UTC", text)


if __name__ == "__main__":
    unittest.main()
