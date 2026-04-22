from __future__ import annotations

from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, MessageHandler, filters

from .config import Config, load_env_file
from .constants import BASE_DIR, ENV_FILE
from .handlers import (
    cancel_command,
    help_command,
    on_shutdown,
    on_startup,
    skip_command,
    start_command,
    status_command,
    text_file_message,
    text_message,
)
from .logging_config import setup_logging
from .store import create_store
from .worker import UploadWorker


def build_application(config: Config) -> Application:
    store = create_store(config)
    builder = (
        ApplicationBuilder()
        .token(config.token)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .connect_timeout(60)
        .read_timeout(600)
        .write_timeout(600)
        .pool_timeout(60)
    )
    application = builder.build()
    application.bot_data["store"] = store
    application.bot_data["uploader"] = UploadWorker(application.bot, store, config)
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler(["skip", "remove_current"], skip_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(MessageHandler(filters.Document.TEXT, text_file_message))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))
    return application


def main() -> None:
    setup_logging()
    load_env_file(ENV_FILE)
    config = Config.from_env(BASE_DIR)
    application = build_application(config)
    application.run_polling(allowed_updates=Update.ALL_TYPES)
