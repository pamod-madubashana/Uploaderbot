from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from uploaderbot.config import Config, TELEGRAM_BOT_UPLOAD_LIMIT_BYTES, TELEGRAM_UPLOAD_SAFETY_MARGIN_BYTES


class ConfigTests(unittest.TestCase):
    def test_from_env_parses_multiple_chat_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            with patch.dict(
                os.environ,
                {
                    "TOKEN": "token",
                    "DATABASE": "sqlite:///state.db",
                    "CHAT_IDs": "-1001,-1002",
                },
                clear=True,
            ):
                config = Config.from_env(base_dir)

        self.assertEqual(config.chat_ids, [-1001, -1002])

    def test_from_env_caps_download_size_below_telegram_upload_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            with patch.dict(
                os.environ,
                {
                    "TOKEN": "token",
                    "DATABASE": "sqlite:///state.db",
                    "CHAT_IDs": "-1001",
                    "MAX_DOWNLOAD_SIZE_MB": "50",
                },
                clear=True,
            ):
                config = Config.from_env(base_dir)

        self.assertEqual(
            config.max_download_size_bytes,
            TELEGRAM_BOT_UPLOAD_LIMIT_BYTES - TELEGRAM_UPLOAD_SAFETY_MARGIN_BYTES,
        )


if __name__ == "__main__":
    unittest.main()
