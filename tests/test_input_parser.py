from __future__ import annotations

import unittest

from uploaderbot.input_parser import parse_queue_text


class ParseQueueTextTests(unittest.TestCase):
    def test_parses_single_link(self) -> None:
        self.assertEqual(
            parse_queue_text("example.com/1/1.mp4"),
            ["https://example.com/1/1.mp4"],
        )

    def test_expands_last_number_range(self) -> None:
        self.assertEqual(
            parse_queue_text("https://example.com/1/1.mp4 1-3"),
            [
                "https://example.com/1/1.mp4",
                "https://example.com/1/2.mp4",
                "https://example.com/1/3.mp4",
            ],
        )

    def test_expands_shared_placeholder_range(self) -> None:
        self.assertEqual(
            parse_queue_text("https://example.com/{n}/{n}.mp4 1-3"),
            [
                "https://example.com/1/1.mp4",
                "https://example.com/2/2.mp4",
                "https://example.com/3/3.mp4",
            ],
        )

    def test_expands_named_placeholder_assignment(self) -> None:
        self.assertEqual(
            parse_queue_text("https://example.com/{folder}/{file}.mp4 folder=1-3 file=2"),
            [
                "https://example.com/1/2.mp4",
                "https://example.com/2/2.mp4",
                "https://example.com/3/2.mp4",
            ],
        )

    def test_expands_block1000_placeholder_from_range(self) -> None:
        self.assertEqual(
            parse_queue_text(
                "https://example.com/{block1000:n}/{n}/{n}.mp4 2000-2002"
            ),
            [
                "https://example.com/2000/2000/2000.mp4",
                "https://example.com/2000/2001/2001.mp4",
                "https://example.com/2000/2002/2002.mp4",
            ],
        )

    def test_expands_block1000_placeholder_across_block_boundary(self) -> None:
        self.assertEqual(
            parse_queue_text(
                "https://example.com/{block1000:n}/{n}/{n}.mp4 2999-3001"
            ),
            [
                "https://example.com/2000/2999/2999.mp4",
                "https://example.com/3000/3000/3000.mp4",
                "https://example.com/3000/3001/3001.mp4",
            ],
        )

    def test_expands_index1000_placeholder_across_block_boundary(self) -> None:
        self.assertEqual(
            parse_queue_text(
                "https://example.com/{block1000:n}/{index1000:n}/{index1000:n}.mp4 999-1001"
            ),
            [
                "https://example.com/0/999/999.mp4",
                "https://example.com/1000/1000/1000.mp4",
                "https://example.com/1000/1001/1001.mp4",
            ],
        )

    def test_expands_offset1000_placeholder_across_block_boundary(self) -> None:
        self.assertEqual(
            parse_queue_text(
                "https://example.com/{block1000:n}/{offset1000:n}/{offset1000:n}.mp4 999-1001"
            ),
            [
                "https://example.com/0/999/999.mp4",
                "https://example.com/1000/1000/1000.mp4",
                "https://example.com/1000/1/1.mp4",
            ],
        )


if __name__ == "__main__":
    unittest.main()
