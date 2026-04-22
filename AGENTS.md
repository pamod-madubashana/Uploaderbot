# AGENTS.md

This file gives coding agents a compact operating guide for this repository.

## Project Overview

- Project type: async Python Telegram bot.
- Main purpose: accept media source links, queue downloads, upload media to Telegram, and track queue state.
- Runtime stack: `python-telegram-bot`, `httpx`, `pymongo`, SQLite fallback.
- Package root: `uploaderbot/`.
- Default launcher in the repo root: `__main__.py`.
- Core modules:
  - `uploaderbot/app.py` builds the Telegram application.
  - `uploaderbot/handlers.py` handles Telegram commands, text messages, text-file submissions, and progress updates.
  - `uploaderbot/worker.py` runs the upload loop.
  - `uploaderbot/store.py` provides MongoDB and SQLite queue/state backends.
  - `uploaderbot/input_parser.py` parses single links, text-file lines, and URL patterns.
  - `uploaderbot/downloader.py` streams remote files to disk.
  - `uploaderbot/media.py` holds URL/media helpers.

## Environment And Startup

- Required env vars are loaded from `.env` by `uploaderbot/config.py`.
- Important env vars:
  - `TOKEN`
  - `CHAT_ID`
  - `DATABASE`
- Optional env vars:
  - `DATABASE_NAME`
  - `DOWNLOAD_DIR`
  - `RETRY_DELAY_SECONDS`
  - `SQLITE_DB_FILE`
- MongoDB is preferred when `DATABASE` points to Mongo, but the app falls back to SQLite if Mongo connection fails.
- To force SQLite, use `DATABASE=sqlite:///upload_state.db`.

## Build / Check / Test Commands

- There is no separate build pipeline or lint config checked into this repo.
- The closest build/sanity step is Python bytecode compilation.

### Install dependencies

```bash
python -m pip install -r requirements.txt
```

### Run the bot locally

```bash
python __main__.py
```

If the repo still contains `bot.py` in the branch you are working on, `python bot.py` is functionally equivalent.

### Compile all Python files

```bash
python -m compileall uploaderbot tests
```

### Run the full test suite

```bash
python -m unittest discover -s tests
```

### Run a single test module

```bash
python -m unittest tests.test_input_parser
python -m unittest tests.test_store
```

### Run a single test class

```bash
python -m unittest tests.test_input_parser.ParseQueueTextTests
python -m unittest tests.test_store.StoreTests
```

### Run a single test method

```bash
python -m unittest tests.test_input_parser.ParseQueueTextTests.test_expands_shared_placeholder_range
python -m unittest tests.test_store.StoreTests.test_create_store_falls_back_when_mongo_fails
```

## What To Verify After Code Changes

- For parser or store logic changes: run `python -m unittest discover -s tests`.
- For handler/worker changes: run tests and `python -m compileall uploaderbot tests`.
- For application wiring changes: ensure imports still resolve and handlers still register in `uploaderbot/app.py`.
- For queue/state changes: verify both MongoDB and SQLite code paths remain valid.
- For Telegram progress-message changes: make sure the update loop still tolerates `message is not modified` errors.

## Code Style Guidelines

### Python Version / General Style

- Follow the existing Python 3.11+ style already used in the repo.
- Keep code simple and explicit; this codebase favors readability over clever abstractions.
- Prefer ASCII when editing unless the file already relies on Unicode characters intentionally.
- Use concise helper functions for repeated formatting/parsing logic.

### Imports

- Keep `from __future__ import annotations` at the top of Python modules that already use it.
- Group imports in this order:
  1. standard library
  2. third-party packages
  3. local `uploaderbot` imports
- Separate groups with a single blank line.
- Import only what is used.
- Prefer direct imports over wildcard imports.

### Formatting

- Follow the repository’s current formatting style, which is close to Black defaults even though Black is not configured.
- Use 4-space indentation.
- Keep line length reasonable; wrap long calls with hanging indents as seen in `handlers.py` and `store.py`.
- Use trailing commas in multiline literals/calls when it improves diff quality.
- Preserve one blank line between top-level definitions.

### Types

- Add type hints for new functions, methods, and important locals.
- Use built-in generics like `list[str]`, `dict[str, Any]`, and `tuple[int, int]`.
- Use `Protocol` when defining backend contracts across multiple implementations.
- Use `object` rather than `Any` when a value is intentionally opaque.
- Prefer explicit return types on public helpers and class methods.

### Naming

- Use `snake_case` for functions, methods, variables, and module-level helpers.
- Use `CamelCase` for classes.
- Use `UPPER_SNAKE_CASE` for module constants.
- Choose names that match the queue/upload domain: `enqueue_urls`, `get_batch_progress`, `mark_uploaded`, etc.
- Keep handler names action-oriented: `text_message`, `text_file_message`, `status_command`.

### Async And Concurrency

- Telegram handlers are async; keep network-facing handlers as `async def`.
- Run blocking DB/file operations through `asyncio.to_thread(...)` as the current code does.
- Background loops should be resilient and log failures instead of failing silently.
- When adding background tasks, register done callbacks so exceptions are logged.
- Be careful not to start duplicate worker/progress tasks.

### Error Handling

- Raise specific, readable exceptions for invalid user input, as in `QueueInputError`.
- Catch service-boundary exceptions close to the boundary:
  - `TelegramError` around Telegram API operations
  - `PyMongoError` around Mongo connection/setup
  - decode errors around uploaded text files
- Preserve the current graceful fallback behavior from MongoDB to SQLite.
- Do not swallow exceptions silently; either log them or convert them to a user-facing Telegram reply.
- Keep error messages short and actionable for end users.

### Logging

- Use the module logger pattern: `logger = logging.getLogger("uploaderbot")`.
- Log lifecycle events for queueing, downloading, uploading, retries, and cleanup.
- Prefer structured log messages with placeholders instead of f-strings in logger calls.
- Log enough context to debug queue position and source URL, but avoid noisy per-byte logging.

### Data / Storage Patterns

- Keep MongoDB and SQLite implementations behaviorally aligned.
- When changing store behavior, update both backends unless the change is intentionally backend-specific.
- Store records use line numbers as stable queue ordering.
- State refresh logic should continue to expose counts, current item, next item, and last error.

### Telegram UX Conventions

- Reply directly to the user’s source message when acknowledging queued input.
- Keep `/start` and `/status` responses concise.
- Progress messages should remain readable in plain text.
- If editing a message repeatedly, tolerate benign “message is not modified” failures.

### Tests

- Add or update unit tests when changing parser rules, store behavior, or fallback logic.
- Keep tests in `tests/` using `unittest`.
- Name test files `test_*.py`.
- Name test methods after the behavior they verify.
- Prefer focused tests for parser edge cases and backend fallback behavior.

## Repo-Specific Rules Files

- No `.cursorrules` file was found.
- No `.cursor/rules/` directory was found.
- No `.github/copilot-instructions.md` file was found.
- If any of these files are added later, update this document to summarize their instructions.

## Practical Agent Advice

- Before editing store logic, inspect both backend implementations.
- Before editing handlers, trace how `uploaderbot/app.py` registers them.
- Before changing queue parsing, review the tests in `tests/test_input_parser.py` and add new cases.
- Prefer small, targeted edits over broad refactors.
- Avoid introducing new tooling unless the repository already adopts it.
