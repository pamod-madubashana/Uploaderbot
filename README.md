# Telegram Uploader Bot

This bot accepts links directly from Telegram messages, downloads the media, and uploads it to your target chat.

## Features

- Send a single link and the bot queues it immediately.
- Send a UTF-8 text file and the bot queues every supported link inside it.
- Send link patterns and expand them into many URLs.
- Get a reply on the source message with a progress bar that refreshes every 10 seconds.
- Works with MongoDB when available and falls back to SQLite automatically.

## Setup

Copy `.env.sample` to `.env`, then fill in your real values:

```bash
copy .env.sample .env
```

Use this template:

```env
TOKEN=your_bot_token
CHAT_ID=-1001234567890
DATABASE=mongodb://localhost:27017
DATABASE_NAME=telegram_uploader
DOWNLOAD_DIR=downloads
MAX_DOWNLOAD_SIZE_MB=50
RETRY_DELAY_SECONDS=60
SQLITE_DB_FILE=upload_state.db
```

Notes:

- To force SQLite only, set `DATABASE=sqlite:///upload_state.db`.
- If `DATABASE` points to MongoDB and the connection fails, the bot uses SQLite automatically.
- Files larger than `MAX_DOWNLOAD_SIZE_MB` are skipped before the full download finishes.

## Run

```bash
python __main__.py
```

Or use the helper script on Linux/macOS to pull the latest code, create `.venv`, install missing requirements, and start the bot:

```bash
bash run.sh
```

## Telegram Inputs

The bot accepts these message formats:

```text
https://example.com/video.mp4
example.com/video.mp4
https://example.com/1/1.mp4 1-100
https://example.com/{n}/2.mp4 1-100
https://example.com/{n}/{n}.mp4 1-100
https://example.com/{block1000:n}/{n}/{n}.mp4 2000-129000
https://example.com/{block1000:n}/{index1000:n}/{index1000:n}.mp4 1-129000
https://example.com/{block1000:n}/{offset1000:n}/{offset1000:n}.mp4 1-129000
https://example.com/{folder}/{file}.mp4 folder=1-100 file=2
```

You can also upload a UTF-8 `.txt` file with one entry per line. Each line may contain:

- a single link
- multiple links
- a ranged pattern
- a placeholder pattern

Examples:

```text
https://example.com/episode1.mp4
https://example.com/{n}/2.mp4 1-100
https://example.com/{block1000:n}/{n}/{n}.mp4 2000-129000
https://example.com/{block1000:n}/{index1000:n}/{index1000:n}.mp4 1-129000
https://example.com/{block1000:n}/{offset1000:n}/{offset1000:n}.mp4 1-129000
https://example.com/{folder}/{file}.mp4 folder=1-100 file=2
```

## Commands

- `/start` shows the supported input formats.
- `/help` shows all available commands.
- `/status` shows the global queue status.
- `/skip` removes the item that is currently downloading or uploading.
- `/remove_current` is an alias for `/skip`.
- `/cancel` removes all current and queued non-uploaded items.

## Progress Updates

When you send a source link or a text file, the bot replies to that message and updates the reply every 10 seconds with:

- queue status
- progress bar
- uploaded count
- current item
- next item
- last error

## Tests

```bash
python -m unittest discover -s tests
```
