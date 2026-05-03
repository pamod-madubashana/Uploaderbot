from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from telegram import BotCommand, Message, Update
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

BOT_COMMANDS = [
    BotCommand("start", "show a quick welcome message"),
    BotCommand("help", "show all available commands"),
    BotCommand("status", "show current queue progress"),
    BotCommand("skip", "remove the current item"),
    BotCommand("remove_current", "alias for /skip"),
    BotCommand("cancel", "remove all current and queued items"),
]


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

    await _start_text_submission(
        message,
        context,
        source_label="message",
        text=message.text,
    )


async def text_file_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None or message.document is None:
        return

    filename = message.document.file_name or "file"
    placeholder_message = await message.reply_text("Preparing file input...")
    start_submission_task(
        context.application,
        submission_message=placeholder_message,
        coroutine=_process_text_file_submission(
            message=message,
            application=context.application,
            source_label=filename,
            submission_message=placeholder_message,
        ),
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return

    store = context.application.bot_data["store"]
    state = await asyncio.to_thread(store.get_state)
    await _replace_chat_progress_watch(context.application, chat_id=message.chat_id)
    progress_message = await message.reply_text(
        _format_progress_message(source_label="status", queue_state=state),
        disable_web_page_preview=True,
    )
    start_progress_task(
        context.application,
        chat_id=progress_message.chat_id,
        message_id=progress_message.message_id,
        source_label="status",
        first_line_number=0,
        last_line_number=0,
    )


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
    text: str,
    application: Application,
    submission_message: Message,
    *,
    source_label: str,
) -> None:
    store = application.bot_data["store"]

    try:
        urls = await asyncio.to_thread(parse_queue_text, text)
    except QueueInputError as exc:
        await _edit_submission_message(submission_message, f"Could not parse input: {exc}")
        return

    if not urls:
        await _edit_submission_message(
            submission_message,
            "No links found. Send one link per line, a single link, or a pattern like https://example.com/{n}/2.mp4 1-100."
        )
        return

    enqueue_result = await asyncio.to_thread(store.enqueue_urls, urls)
    state = enqueue_result["state"]
    first_line_number = enqueue_result["first_line_number"]
    last_line_number = enqueue_result["last_line_number"]

    await _replace_chat_progress_watch(
        application,
        chat_id=submission_message.chat_id,
        keep_message_id=submission_message.message_id,
    )

    await _edit_submission_message(
        submission_message,
        _format_progress_message(
            source_label=source_label,
            queue_state=state,
        ),
    )

    start_upload_task(application)

    if first_line_number is not None and last_line_number is not None:
        start_progress_task(
            application,
            chat_id=submission_message.chat_id,
            message_id=submission_message.message_id,
            source_label=source_label,
            first_line_number=first_line_number,
            last_line_number=last_line_number,
        )


async def _start_text_submission(
    message: Message,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    source_label: str,
    text: str,
) -> None:
    placeholder_message = await message.reply_text("Processing input...")
    start_submission_task(
        context.application,
        submission_message=placeholder_message,
        coroutine=_queue_text_payload(
            text,
            context.application,
            placeholder_message,
            source_label=source_label,
        ),
    )


async def _process_text_file_submission(
    *,
    message: Message,
    application: Application,
    source_label: str,
    submission_message: Message,
) -> None:
    if message.document is None:
        await _edit_submission_message(submission_message, "Could not read the uploaded file.")
        return

    try:
        telegram_file = await message.document.get_file()
        payload = await telegram_file.download_as_bytearray()
    except TelegramError as exc:
        await _edit_submission_message(submission_message, f"Could not download the text file: {exc}")
        return

    try:
        text = bytes(payload).decode("utf-8-sig")
    except UnicodeDecodeError:
        await _edit_submission_message(submission_message, "Text files must be UTF-8 encoded.")
        return

    await _queue_text_payload(
        text,
        application,
        submission_message,
        source_label=source_label,
    )


def start_submission_task(
    application: Application,
    *,
    submission_message: Message,
    coroutine: Any,
) -> None:
    submission_tasks = application.bot_data.setdefault("submission_tasks", {})
    task = asyncio.create_task(
        coroutine,
        name=f"submission-{submission_message.message_id}",
    )
    task.add_done_callback(log_background_task)
    task.add_done_callback(lambda finished: submission_tasks.pop(submission_message.message_id, None))
    submission_tasks[submission_message.message_id] = task


async def _edit_submission_message(message: Message, text: str) -> None:
    try:
        await message.edit_text(text, disable_web_page_preview=True)
    except BadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            raise


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
    chat_id: int,
    message_id: int,
    source_label: str,
    first_line_number: int,
    last_line_number: int,
) -> None:
    progress_tasks = application.bot_data.setdefault("progress_tasks", {})
    if message_id in progress_tasks and not progress_tasks[message_id].done():
        return

    store = application.bot_data["store"]
    store.save_progress_watch(
        chat_id=chat_id,
        message_id=message_id,
        source_label=source_label,
        first_line_number=first_line_number,
        last_line_number=last_line_number,
    )
    task = asyncio.create_task(
        monitor_batch_progress(
            application,
            chat_id=chat_id,
            message_id=message_id,
            source_label=source_label,
            first_line_number=first_line_number,
            last_line_number=last_line_number,
        ),
        name=f"progress-{message_id}",
    )
    task.add_done_callback(log_background_task)
    task.add_done_callback(lambda finished: progress_tasks.pop(message_id, None))
    progress_tasks[message_id] = task


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
    displayed_updated_at: datetime | None = None
    is_status_watch = source_label == "status"

    while True:
        batch_progress: dict[str, Any] | None = None
        if not is_status_watch:
            batch_progress = await asyncio.to_thread(
                store.get_batch_progress,
                first_line_number,
                last_line_number,
            )
        queue_state = await asyncio.to_thread(store.get_state)
        text = _format_progress_message(
            source_label=source_label,
            queue_state=queue_state,
            updated_at=displayed_updated_at,
        )

        try:
            updated_message = await application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                disable_web_page_preview=True,
            )
            displayed_updated_at = _resolve_message_updated_at(updated_message)
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                await asyncio.to_thread(store.delete_progress_watch, message_id)
                raise
        except TelegramError as exc:
            await asyncio.to_thread(store.delete_progress_watch, message_id)
            logger.warning("Could not update progress message %s: %s", message_id, exc)
            return

        if is_status_watch:
            await asyncio.sleep(PROGRESS_UPDATE_SECONDS)
            continue

        assert batch_progress is not None
        if batch_progress.get("total_count", 0) == 0 or batch_progress.get("uploaded_count") == batch_progress.get("total_count"):
            await asyncio.to_thread(store.delete_progress_watch, message_id)
            return

        await asyncio.sleep(PROGRESS_UPDATE_SECONDS)


async def _replace_chat_progress_watch(
    application: Application,
    *,
    chat_id: int,
    keep_message_id: int | None = None,
) -> None:
    store = application.bot_data["store"]
    progress_tasks = application.bot_data.setdefault("progress_tasks", {})
    watches = await asyncio.to_thread(store.list_progress_watches)

    for watch in watches:
        watch_chat_id = int(watch.get("chat_id", 0) or 0)
        watch_message_id = int(watch.get("message_id", 0) or 0)
        if watch_chat_id != chat_id or watch_message_id == keep_message_id:
            continue

        existing_task = progress_tasks.get(watch_message_id)
        if existing_task is not None and not existing_task.done():
            existing_task.cancel()
            try:
                await existing_task
            except asyncio.CancelledError:
                pass

        await asyncio.to_thread(store.delete_progress_watch, watch_message_id)

        try:
            await application.bot.delete_message(chat_id=chat_id, message_id=watch_message_id)
        except BadRequest as exc:
            if "message to delete not found" not in str(exc).lower():
                logger.warning("Could not delete previous progress message %s: %s", watch_message_id, exc)
        except TelegramError as exc:
            logger.warning("Could not delete previous progress message %s: %s", watch_message_id, exc)


def _format_progress_message(
    *,
    source_label: str,
    queue_state: dict[str, Any],
    updated_at: datetime | None = None,
) -> str:
    total_count = int(queue_state.get("total_count", 0) or 0)
    uploaded_count = int(queue_state.get("uploaded_count", 0) or 0)
    status = str(queue_state.get("status", "ready"))
    database_name = DATABASE_LABELS.get(str(queue_state.get("backend", "unknown")), str(queue_state.get("backend", "unknown")))
    current_line = queue_state.get("current_line_number")
    current_url = queue_state.get("current_url")
    next_line = queue_state.get("next_line_number")
    next_url = queue_state.get("next_url")
    last_error = _short_error(queue_state.get("last_error"))
    percent = 100 if total_count == 0 else int((uploaded_count / total_count) * 100)
    updated_at_text = _format_updated_at(updated_at)

    lines = [
        f"Source: {source_label}",
        f"Database: {database_name}",
        f"Status: {status}",
        f"Progress: {_render_progress_bar(uploaded_count, total_count)} {uploaded_count}/{total_count} ({percent}%)",
        f"Current: {_line_preview(current_line, current_url)}",
        f"Next: {_line_preview(next_line, next_url)}",
        f"Last error: {last_error}",
        f"Updated: {updated_at_text}",
    ]
    return "\n".join(lines)


def _resolve_message_updated_at(updated_message: object) -> datetime | None:
    edit_date = getattr(updated_message, "edit_date", None)
    if isinstance(edit_date, datetime):
        return edit_date.astimezone(timezone.utc)

    sent_date = getattr(updated_message, "date", None)
    if isinstance(sent_date, datetime):
        return sent_date.astimezone(timezone.utc)

    return datetime.now(timezone.utc)


def _format_updated_at(updated_at: datetime | None) -> str:
    if updated_at is None:
        updated_at = datetime.now(timezone.utc)
    return updated_at.astimezone(timezone.utc).strftime("%H:%M:%S UTC")


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
    application.bot_data.setdefault("submission_tasks", {})
    await ensure_bot_commands(application)
    await restore_progress_tasks(application)
    start_upload_task(application)


async def ensure_bot_commands(application: Application) -> None:
    try:
        existing_commands = await application.bot.get_my_commands()
    except TelegramError as exc:
        logger.warning("Could not read bot commands during startup: %s", exc)
        return

    if existing_commands:
        return

    try:
        await application.bot.set_my_commands(BOT_COMMANDS)
    except TelegramError as exc:
        logger.warning("Could not set bot commands during startup: %s", exc)


async def restore_progress_tasks(application: Application) -> None:
    store = application.bot_data["store"]
    latest_watches_by_chat: dict[int, dict[str, Any]] = {}
    for progress_watch in store.list_progress_watches():
        latest_watches_by_chat[int(progress_watch["chat_id"])] = progress_watch

    for progress_watch in latest_watches_by_chat.values():
        await _replace_chat_progress_watch(
            application,
            chat_id=int(progress_watch["chat_id"]),
            keep_message_id=int(progress_watch["message_id"]),
        )
        start_progress_task(
            application,
            chat_id=int(progress_watch["chat_id"]),
            message_id=int(progress_watch["message_id"]),
            source_label=str(progress_watch["source_label"]),
            first_line_number=int(progress_watch["first_line_number"]),
            last_line_number=int(progress_watch["last_line_number"]),
        )


async def on_shutdown(application: Application) -> None:
    submission_tasks = application.bot_data.get("submission_tasks", {})
    for task in list(submission_tasks.values()):
        if not task.done():
            task.cancel()

    for task in list(submission_tasks.values()):
        if task.done():
            continue
        try:
            await task
        except asyncio.CancelledError:
            pass

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
