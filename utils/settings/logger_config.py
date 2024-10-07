import sys
import asyncio
import threading
from pathlib import Path
from typing import Optional, Dict, Any, Union
from loguru import logger
import telegram
from telegram.constants import ParseMode
from queue import Queue
import html


class TelegramWorker:
    def __init__(self, bot_token: str, chat_id: str, channels: Dict[str, int]):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.channels = channels
        self.queue = Queue()
        self.bot = telegram.Bot(token=self.bot_token)
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.start_worker, daemon=True)
        self.thread.start()

    def start_worker(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.process_queue())
        finally:
            self.loop.close()

    async def process_queue(self):
        while True:
            message, channel = await self.loop.run_in_executor(None, self.queue.get)
            if message is None and channel is None:
                self.queue.task_done()
                break
            await self.send_message_async(message, channel)
            self.queue.task_done()

    async def send_message_async(self, message: str, channel: str) -> None:
        kwargs = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": ParseMode.HTML
        }
        try:
            message_thread_id = self.channels.get(channel)
            if message_thread_id and message_thread_id > 0:
                kwargs["message_thread_id"] = message_thread_id

            await self.bot.send_message(**kwargs)
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e} | Message: {message} | Channel: {channel}")

    def send_message(self, message: str, channel: str) -> None:
        self.queue.put((message, channel))

    def stop(self):
        self.queue.put((None, None))
        self.queue.join()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()


class AsyncTelegramHandler:
    def __init__(self, telegram_settings: Dict[str, Any]):
        self.enabled = telegram_settings.get('enabled', False)
        if not self.enabled:
            logger.debug("Telegram notifications are disabled.")
            return

        self.bot_token = telegram_settings['bot_token']
        self.chat_id = telegram_settings['chat_id']
        self.channels = {
            channel['name']: channel['message_thread_id']
            for channel in telegram_settings['channels']
        }

        try:
            self.worker = TelegramWorker(self.bot_token, self.chat_id, self.channels)
            future = asyncio.run_coroutine_threadsafe(self.worker.bot.get_me(), self.worker.loop)
            future.result()
            logger.debug("Telegram bot initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Telegram bot: {e}")
            self.enabled = False

    def send_message(self, message: str, channel: str) -> None:
        if not self.enabled:
            logger.debug(f"Telegram disabled, would have sent to channel '{channel}': {message}")
            return
        self.worker.send_message(message, channel)

    def stop(self):
        if self.enabled:
            self.worker.stop()


def setup_logger(
        log_path: Union[str, Path],
        app_name: str = "default",
        exchange_name: Optional[str] = "default",
        telegram_settings: Optional[Dict[str, Any]] = None
) -> Optional[AsyncTelegramHandler]:
    if isinstance(log_path, str):
        log_path = Path(log_path)

    log_path.parent.mkdir(parents=True, exist_ok=True)

    log_format = (
        f"<green>{{time:YYYY-MM-DD HH:mm:ss.SSSZZ}}</green> "
        f"<red>|</red>"
        f"<magenta>{app_name}.{exchange_name}</magenta>"
        f"<red>|</red> " 
        f"<level>{{level: <8}}</level>"
        f"<red>|</red> "
        f"<cyan>{{module}}</cyan><red>:</red><cyan>{{function}}</cyan><red>:</red><cyan>{{line}}</cyan> "
        f"<red>|</red> "
        f"{{message}}"
    )

    logger.remove()

    logger.add(
        str(log_path),
        rotation="100 MB",
        retention="1 week",
        compression="zip",
        level="DEBUG",
        format=log_format,
        enqueue=True
    )

    logger.add(
        sys.stdout,
        level="DEBUG",
        format=log_format,
        colorize=True,
        enqueue=True
    )

    telegram_handler = None
    if telegram_settings and telegram_settings.get('enabled'):
        telegram_handler = AsyncTelegramHandler(telegram_settings)

        def telegram_sink(message):
            record = message.record
            level = record["level"].name

            channel = None
            if level in ("CRITICAL", "ERROR"):
                channel = "ERROR"
            elif "channel" in record["extra"]:
                channel = record["extra"]["channel"]
            elif app_name == "trade" and level not in ('DEBUG'):
                channel = "TRADE"

            if not channel:
                return

            timestamp = record["time"].strftime('%Y-%m-%d %H:%M:%S%z')
            module = html.escape(record["module"])
            function = html.escape(record["function"])
            line = record["line"]
            message_text = html.escape(record["message"])

            if level == "INFO" and (channel == "TRADE" or app_name == "trade"):
                msg = f"{timestamp} |{exchange_name}| \n{message_text}"
            else:
                msg = (
                    f"<b>{timestamp} | {app_name}.{exchange_name} |</b>\n"
                    f"<b>{module}:{function}:{line}</b>"
                    f"\n{message_text}"
                )

            if record["exception"]:
                traceback_obj = record.get("traceback", None)
                if traceback_obj:
                    traceback_str = ''.join(traceback_obj.format())
                    traceback_str = html.escape(traceback_str)
                    msg += f"\n<pre>{traceback_str}</pre>"
                else:
                    exception_type = type(record["exception"][1]).__name__
                    exception_msg = html.escape(str(record["exception"][1]))
                    msg += f"\n<pre>{exception_type}: {exception_msg}</pre>"

            MAX_TELEGRAM_MESSAGE_LENGTH = 4096
            if len(msg) > MAX_TELEGRAM_MESSAGE_LENGTH:
                msg = msg[:MAX_TELEGRAM_MESSAGE_LENGTH - 100] + "\n<pre>Message truncated due to length.</pre>"

            telegram_handler.send_message(msg, channel)

        logger.add(
            telegram_sink,
            level="INFO",
            enqueue=True
        )

    return telegram_handler