# AGENTS.md

This file gives coding agents a practical guide for working in this repository.

## Project Summary

- Project type: async Python Telegram uploader bot.
- Main job: accept links or text files from Telegram, queue downloads, upload media, and track progress.
- Main package: `uploaderbot/`.
- Entry point: `__main__.py`.
- Helper launcher: `run.sh`.
- Storage: MongoDB when available, SQLite fallback otherwise.

## Key Modules

- `uploaderbot/app.py` wires the Telegram application and handlers.
- `uploaderbot/handlers.py` handles commands, text submissions, progress messages, and startup/shutdown hooks.
- `uploaderbot/worker.py` runs the upload loop and retry/cancel logic.
- `uploaderbot/store.py` implements MongoDB and SQLite queue/state storage.
- `uploaderbot/input_parser.py` parses raw links and URL patterns.
- `uploaderbot/downloader.py` downloads source files and enforces the size cap.
- `uploaderbot/mp4.py` prepares MP4 metadata, streaming layout, and thumbnails.
- `uploaderbot/media.py` contains media and time helpers.
- `tests/` holds the unit test suite.

## Environment

- Config is loaded from `.env` via `uploaderbot/config.py`.
- Start from `.env.sample` when setting up a new environment.
- Important env vars:
  - `TOKEN`
  - `CHAT_ID`
  - `DATABASE`
- Common optional env vars:
  - `DATABASE_NAME`
  - `DOWNLOAD_DIR`
  - `MAX_DOWNLOAD_SIZE_MB`
  - `RETRY_DELAY_SECONDS`
  - `SQLITE_DB_FILE`

## Build / Check / Test Commands

There is no dedicated lint or build tool configured in the repo. The normal safety checks are tests plus bytecode compilation.

### Install dependencies

```bash
python -m pip install -r requirements.txt
```

### Run the bot

```bash
python __main__.py
```

### Run with helper script

```bash
bash run.sh
```

`run.sh` pulls the latest fast-forward changes, ensures `.venv` exists, installs missing requirements, and starts the bot.

### Compile all source files

```bash
python -m compileall uploaderbot tests
```

### Run all tests

```bash
python -m unittest discover -s tests
```

### Run a single test module

```bash
python -m unittest tests.test_input_parser
python -m unittest tests.test_store
python -m unittest tests.test_worker
python -m unittest tests.test_mp4
python -m unittest tests.test_handlers
```

### Run a single test class

```bash
python -m unittest tests.test_input_parser.ParseQueueTextTests
python -m unittest tests.test_store.StoreTests
python -m unittest tests.test_worker.UploadWorkerTests
```

### Run a single test method

```bash
python -m unittest tests.test_input_parser.ParseQueueTextTests.test_expands_block1000_placeholder_from_range
python -m unittest tests.test_store.StoreTests.test_enqueue_urls_inserts_new_items_before_existing_pending_items
python -m unittest tests.test_worker.UploadWorkerTests.test_worker_removes_404_item_without_crashing
```

## What To Verify After Changes

- Parser changes: run `python -m unittest tests.test_input_parser`.
- Store changes: run `python -m unittest tests.test_store`.
- Worker/download changes: run `python -m unittest tests.test_worker` and full suite if behavior crosses modules.
- MP4/thumbnail changes: run `python -m unittest tests.test_mp4`.
- Handler/progress-message changes: run `python -m unittest tests.test_handlers` and compileall.
- Any non-trivial change: run the full test suite and `python -m compileall uploaderbot tests`.

## Code Style

### General

- Follow existing Python 3.11+ style.
- Prefer clear, small, direct changes over broad refactors.
- Keep code explicit; avoid clever one-liners when they hurt readability.
- Prefer ASCII unless the file already uses Unicode intentionally.

### Imports

- Keep `from __future__ import annotations` where already used.
- Group imports in this order:
  1. standard library
  2. third-party packages
  3. local imports
- Separate import groups with one blank line.
- Remove unused imports.
- Do not use wildcard imports.

### Formatting

- Use 4-space indentation.
- Match the current style, which is close to Black formatting.
- Keep lines reasonably short; wrap long calls with hanging indents.
- Use trailing commas in multiline calls/literals when it improves diffs.
- Keep one blank line between top-level defs.

### Types

- Add type hints for new public functions, methods, and important helpers.
- Prefer built-in generics such as `list[str]` and `dict[str, Any]`.
- Use `Protocol` for shared backend contracts.
- Use `object` instead of `Any` when a value is intentionally opaque.

### Naming

- `snake_case` for functions, methods, variables, and modules.
- `CamelCase` for classes.
- `UPPER_SNAKE_CASE` for constants.
- Use domain-specific names such as `enqueue_urls`, `get_batch_progress`, `cancel_all_items`, and `prepare_video_file`.

## Async / Concurrency Guidelines

- Telegram handlers should stay `async def`.
- Move blocking file or DB work to `asyncio.to_thread(...)`.
- Background tasks must log exceptions through done callbacks.
- Avoid duplicate worker, submission, or progress tasks for the same purpose.
- Be careful when changing cancellation flow; `/skip` and `/cancel` rely on task cancellation semantics.

## Error Handling

- Raise `QueueInputError` for invalid user-supplied patterns.
- Catch boundary exceptions near the boundary:
  - `TelegramError` for Telegram API calls
  - `httpx.HTTPStatusError` and `httpx.RequestError` for downloads
  - `PyMongoError` for MongoDB setup/fallback
  - `UnicodeDecodeError` for uploaded text files
- Keep user-facing error messages concise.
- Log failures with enough queue/url context to debug them.
- Preserve MongoDB-to-SQLite fallback behavior.

## Storage Rules

- Keep MongoDB and SQLite behavior aligned unless intentionally backend-specific.
- When changing queue logic, update both storage implementations.
- `line_number` is the queue ordering key.
- Progress watch persistence now lives in storage too; remember to update both backends.

## Telegram UX Rules

- Keep `/start`, `/help`, and `/status` concise and readable.
- Source submissions should respond immediately, then continue heavy work in background tasks.
- Progress messages should be editable plain text and tolerate benign "message is not modified" errors.
- `/status` and source progress messages share the same progress format.

## Tests

- Add or update unit tests for parser behavior, store logic, worker retries/cancellation, and MP4 handling.
- Keep tests focused and behavior-driven.
- Test file names should stay `test_*.py`.
- Prefer asserting observable behavior over implementation details.

## Repo-Specific Rules Files

- No `.cursorrules` file was found.
- No `.cursor/rules/` directory was found.
- No `.github/copilot-instructions.md` file was found.
- If any of those files are added later, update this document to summarize them.

## Practical Advice For Agents

- Read both storage backends before editing queue/state behavior.
- Read handler registration in `uploaderbot/app.py` before changing commands.
- Check parser tests before extending placeholder syntax.
- Keep README and `.env.sample` in sync when config or command behavior changes.
- If you change progress-message behavior, think about startup restore, cleanup, and duplicate watchers.
