from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from telegram import Message, Update
from telegram.error import BadRequest, TelegramError
from telegram.ext import Application, ContextTypes

from .input_parser import QueueInputError, parse_queue_text
from .media import short_name_from_url


logger = logging.getLogger("uploaderbot")
PROGRESS_BAR_WIDTH = 12
PROGRESS_UPDATE_SECONDS = 10
MAX_ERROR_LENGTH = 160

DATABASE_LABELS = {
    "mongo": "MongoDB",
    "sqlite": "SQLite",
}


def build_help_text() -> str:
    return (
        "🛠️ Available commands\n\n"
        "▶️ /start - show a quick welcome message\n"
        "❓ /help - show all available commands\n"
        "📊 /status - show current queue progress\n"
        "⏭️ /skip - remove the current item\n"
        "♻️ /remove_current - alias for /skip\n"
        "🧹 /cancel - remove all current and queued items"
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message is None:
        return

    await update.effective_message.reply_text(
        "🎬 Uploader bot is ready.\n\n"
        "📄 Send a text file with links\n"
        "🔗 Send a single direct link\n"
        "🧩 Send a pattern like `site.com/{n}/2.mp4 1-100`\n"
        "🔁 Or use `site.com/{n}/{n}.mp4 1-100`\n\n"
        "📊 `/status` shows queue progress\n"
        "⏭️ `/skip` removes the current item\n"
        "🧹 `/cancel` removes all queued items\n"
        "❓ `/help` shows all commands",
        disable_web_page_preview=True,
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message is None:
        return

    await update.effective_message.reply_text(
        build_help_text(),
        disable_web_page_preview=True,
    )


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None or not message.text:
        return

    await _queue_text_payload(message, message.text, context, source_label="message")


async def text_file_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None or message.document is None:
        return

    try:
        telegram_file = await message.document.get_file()
        payload = await telegram_file.download_as_bytearray()
    except TelegramError as exc:
        await message.reply_text(f"Could not download the text file: {exc}")
        return

    try:
        text = bytes(payload).decode("utf-8-sig")
    except UnicodeDecodeError:
        await message.reply_text("Text files must be UTF-8 encoded.")
        return

    filename = message.document.file_name or "file"
    await _queue_text_payload(message, text, context, source_label=filename)


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message is None:
        return

    store = context.application.bot_data["store"]
    state = await asyncio.to_thread(store.get_state)

    current_line = state.get("current_line_number")
    next_line = state.get("next_line_number")
    last_error = state.get("last_error") or "-"
    database_name = DATABASE_LABELS.get(str(state.get("backend", "unknown")), str(state.get("backend", "unknown")))

    lines = [
        f"Database: {database_name}",
        f"Status: {state.get('status', 'unknown')}",
        f"Uploaded: {state.get('uploaded_count', 0)}/{state.get('total_count', 0)}",
        f"Current line: {current_line if current_line is not None else '-'}",
        f"Next line: {next_line if next_line is not None else '-'}",
        f"Last error: {last_error}",
    ]
    await update.effective_message.reply_text("\n".join(lines))


async def skip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return

    uploader = context.application.bot_data["uploader"]
    skipped_item = await uploader.skip_current_item()
    if skipped_item is None:
        await message.reply_text("No upload is running right now.")
        return

    await message.reply_text(
        f"Removing current item: line {skipped_item['line_number']} - {short_name_from_url(str(skipped_item['url']))}"
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return

    uploader = context.application.bot_data["uploader"]
    result = await uploader.cancel_all_items()
    removed_count = int(result.get("removed_count", 0) or 0)
    if removed_count == 0:
        await message.reply_text("No active queue items to cancel.")
        return

    await message.reply_text(f"Cancelled {removed_count} queued item(s).")


async def _queue_text_payload(
    message: Message,
    text: str,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    source_label: str,
) -> None:
    application = context.application
    store = application.bot_data["store"]

    try:
        urls = parse_queue_text(text)
    except QueueInputError as exc:
        await message.reply_text(f"Could not parse input: {exc}")
        return

    if not urls:
        await message.reply_text(
            "No links found. Send one link per line, a single link, or a pattern like https://example.com/{n}/2.mp4 1-100."
        )
        return

    enqueue_result = await asyncio.to_thread(store.enqueue_urls, urls)
    state = enqueue_result["state"]
    first_line_number = enqueue_result["first_line_number"]
    last_line_number = enqueue_result["last_line_number"]

    progress_message = await message.reply_text(
        _format_batch_progress(
            source_label=source_label,
            progress={
                "status": state.get("status", "ready"),
                "uploaded_count": 0,
                "total_count": len(urls),
                "current_line_number": None,
                "current_url": None,
                "next_line_number": first_line_number,
                "next_url": urls[0],
                "last_error": None,
            },
        )
    )

    start_upload_task(application)

    if first_line_number is not None and last_line_number is not None:
        start_progress_task(
            application,
            progress_message=progress_message,
            source_label=source_label,
            first_line_number=first_line_number,
            last_line_number=last_line_number,
        )


def start_upload_task(application: Application) -> None:
    uploader = application.bot_data["uploader"]
    uploader.notify_queue_changed()

    existing_task = application.bot_data.get("upload_task")
    if existing_task and not existing_task.done():
        return

    task = asyncio.create_task(uploader.run(), name="upload-worker")
    task.add_done_callback(log_background_task)
    application.bot_data["upload_task"] = task


def start_progress_task(
    application: Application,
    *,
    progress_message: Message,
    source_label: str,
    first_line_number: int,
    last_line_number: int,
) -> None:
    progress_tasks = application.bot_data.setdefault("progress_tasks", {})
    task = asyncio.create_task(
        monitor_batch_progress(
            application,
            chat_id=progress_message.chat_id,
            message_id=progress_message.message_id,
            source_label=source_label,
            first_line_number=first_line_number,
            last_line_number=last_line_number,
        ),
        name=f"progress-{progress_message.message_id}",
    )
    task.add_done_callback(log_background_task)
    task.add_done_callback(lambda finished: progress_tasks.pop(progress_message.message_id, None))
    progress_tasks[progress_message.message_id] = task


async def monitor_batch_progress(
    application: Application,
    *,
    chat_id: int,
    message_id: int,
    source_label: str,
    first_line_number: int,
    last_line_number: int,
) -> None:
    store = application.bot_data["store"]

    while True:
        progress = await asyncio.to_thread(
            store.get_batch_progress,
            first_line_number,
            last_line_number,
        )
        text = _format_batch_progress(source_label=source_label, progress=progress)

        try:
            await application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
            )
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                raise
        except TelegramError as exc:
            logger.warning("Could not update progress message %s: %s", message_id, exc)
            return

        if progress.get("total_count", 0) == 0 or progress.get("uploaded_count") == progress.get("total_count"):
            return

        await asyncio.sleep(PROGRESS_UPDATE_SECONDS)


def _format_batch_progress(*, source_label: str, progress: dict[str, Any]) -> str:
    total_count = int(progress.get("total_count", 0) or 0)
    uploaded_count = int(progress.get("uploaded_count", 0) or 0)
    status = str(progress.get("status", "ready"))
    current_line = progress.get("current_line_number")
    current_url = progress.get("current_url")
    next_line = progress.get("next_line_number")
    next_url = progress.get("next_url")
    last_error = _short_error(progress.get("last_error"))
    percent = 100 if total_count == 0 else int((uploaded_count / total_count) * 100)
    updated_at = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    lines = [
        f"Source: {source_label}",
        f"Status: {status}",
        f"Progress: {_render_progress_bar(uploaded_count, total_count)} {uploaded_count}/{total_count} ({percent}%)",
        f"Current: {_line_preview(current_line, current_url)}",
        f"Next: {_line_preview(next_line, next_url)}",
        f"Last error: {last_error}",
        f"Updated: {updated_at}",
    ]
    return "\n".join(lines)


def _render_progress_bar(uploaded_count: int, total_count: int) -> str:
    percent = 0 if total_count <= 0 else (uploaded_count / total_count) * 100
    return f"[{progress_bar(percent)}]"


def progress_bar(pct: object) -> str:
    try:
        pct_value = float(str(pct).strip("%"))
    except ValueError:
        pct_value = 0

    bounded_pct = min(max(pct_value, 0), 100)
    complete_cells = int(bounded_pct // 8)
    partial_cell = int(bounded_pct % 8 - 1)
    bar = "■" * complete_cells

    if complete_cells < PROGRESS_BAR_WIDTH and partial_cell >= 0:
        bar += ["▤", "▥", "▦", "▧", "▨", "▩", "■"][partial_cell]

    if len(bar) > PROGRESS_BAR_WIDTH:
        bar = bar[:PROGRESS_BAR_WIDTH]

    bar += "□" * (PROGRESS_BAR_WIDTH - len(bar))
    return bar


def _line_preview(line_number: object, url: object) -> str:
    if line_number is None:
        return "-"
    if not isinstance(url, str) or not url:
        return str(line_number)
    return f"{line_number} - {short_name_from_url(url)}"


def _short_error(error: object) -> str:
    if not error:
        return "-"
    text = str(error)
    if len(text) <= MAX_ERROR_LENGTH:
        return text
    return f"{text[: MAX_ERROR_LENGTH - 3]}..."


def log_background_task(task: asyncio.Task[Any]) -> None:
    if task.cancelled():
        return

    exception = task.exception()
    if exception is not None:
        logger.error("Background task failed: %s", exception, exc_info=exception)


async def on_startup(application: Application) -> None:
    application.bot_data.setdefault("progress_tasks", {})
    start_upload_task(application)


async def on_shutdown(application: Application) -> None:
    progress_tasks = application.bot_data.get("progress_tasks", {})
    for task in list(progress_tasks.values()):
        if not task.done():
            task.cancel()

    for task in list(progress_tasks.values()):
        if task.done():
            continue
        try:
            await task
        except asyncio.CancelledError:
            pass

    task = application.bot_data.get("upload_task")
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    store = application.bot_data.get("store")
    if store is not None:
        store.close()
