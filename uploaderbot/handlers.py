from __future__ import annotations

import asyncio
import logging
from typing import Any

from telegram import Update
from telegram.ext import Application, ContextTypes


logger = logging.getLogger("uploaderbot")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message is None:
        return

    await update.effective_message.reply_text(
        "Uploader is running automatically. Use /status to see current progress."
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message is None:
        return

    store = context.application.bot_data["store"]
    state = await asyncio.to_thread(store.get_state)

    current_line = state.get("current_line_number")
    next_line = state.get("next_line_number")
    last_error = state.get("last_error") or "-"

    lines = [
        f"Backend: {state.get('backend', 'unknown')}",
        f"Status: {state.get('status', 'unknown')}",
        f"Uploaded: {state.get('uploaded_count', 0)}/{state.get('total_count', 0)}",
        f"Current line: {current_line if current_line is not None else '-'}",
        f"Next line: {next_line if next_line is not None else '-'}",
        f"Last error: {last_error}",
    ]
    await update.effective_message.reply_text("\n".join(lines))


def start_upload_task(application: Application) -> None:
    existing_task = application.bot_data.get("upload_task")
    if existing_task and not existing_task.done():
        return

    uploader = application.bot_data["uploader"]
    task = asyncio.create_task(uploader.run(), name="upload-worker")
    task.add_done_callback(log_background_task)
    application.bot_data["upload_task"] = task


def log_background_task(task: asyncio.Task[Any]) -> None:
    if task.cancelled():
        return

    exception = task.exception()
    if exception is not None:
        logger.error("Background upload task failed: %s", exception, exc_info=exception)


async def on_startup(application: Application) -> None:
    start_upload_task(application)


async def on_shutdown(application: Application) -> None:
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
