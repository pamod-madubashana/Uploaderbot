from __future__ import annotations

import unittest

from uploaderbot.handlers import _format_progress_message


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


if __name__ == "__main__":
    unittest.main()
