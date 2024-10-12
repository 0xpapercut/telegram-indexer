import os
import asyncio
import logging
import json
import datetime

from telethon import TelegramClient, events, sync, types
from telethon.tl.functions.channels import GetFullChannelRequest

from rich.progress import Progress, SpinnerColumn, ProgressColumn, TextColumn
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.text import Text

from .database import DatabaseManager
from .server import WebSocketManager
from .utils import async_enumerate

API_ID = os.environ['API_ID']
API_HASH = os.environ['API_HASH']

DEFAULT_DABASE_PATH = os.path.expanduser('~/.telegram_db/telegram_db.sqlite3')
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 5123

logging.basicConfig(
    level=logging.WARN,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('telegram_db.log')
    ]
)

class TelegramManager:

    def __init__(self):
        self.client = TelegramClient('user', API_ID, API_HASH)
        self.client.add_event_handler(self.new_message_handler, events.NewMessage)
        self.database = DatabaseManager(DEFAULT_DABASE_PATH)
        self.websocket = WebSocketManager(DEFAULT_HOST, DEFAULT_PORT)

        self.lock = asyncio.Lock()
        self.is_catching_up = False
        self.chat_processing_queue = asyncio.Queue()

        self.layout = Layout()
        self.layout.split_column(
            Layout(name='left'),
            Layout(name='right'),
        )
        self.live = Live(self.layout)

    async def run(self):
        logging.info('Starting TelegramManager')
        asyncio.create_task(self.websocket.run())
        await self.database.start()
        await self.client.start()
        asyncio.create_task(self.main_loop())

    async def stop(self):
        await self.client.disconnect()
        await self.database.stop()
        await self.websocket.stop()

    async def main_loop(self):
        scheduler_task = asyncio.create_task(self.chat_processing_scheduler_loop())
        await scheduler_task

    async def new_message_handler(self, event):
        message = event.message
        chat = message.chat
        sender = message.sender

        # self.live.update(Text(f"Last new message received at {datetime.datetime.now()}"))

        if message is None:
            return

        ws_message = {
            'text': message.text,
            'chat_title': chat.title,
            'chat_is_group': message.is_group,
            'chat_is_channel': message.is_channel,
            'chat_is_user': isinstance(chat, types.User),
        }
        if isinstance(sender, types.User):
            username = str(sender.username or (sender.usernames[0].username if sender.usernames else None))
            ws_message['sender'] = {
                'type': 'user',
                'username': username,
                'first_name': sender.first_name,
                'last_name': sender.last_name,
                'is_bot': sender.bot,
                'is_premimum': sender.premium,
                'is_verified': sender.verified,
            }
        else:
            ws_message['sender'] = {
                'type': 'channel',
            }
        await self.websocket.broadcast(json.dumps(ws_message))

        await self.database.insert_message(
            message_id=message.id,
            user_id=sender.id if isinstance(sender, types.User) else None,
            chat_id=message.chat_id,
            text=message.text,
            date=message.date,
        )
        await self.database.insert_chat(
            chat_id=message.chat_id,
            is_channel=message.is_channel,
            is_group=message.is_group,
            is_user=isinstance(chat, types.User),
            title=chat.title,
        )
        if isinstance(sender, types.User):
            username = str(sender.username or (sender.usernames[0].username if sender.usernames else None))
            await self.database.insert_user(
                user_id=sender.id,
                username=username,
                first_name=sender.first_name,
                last_name=sender.last_name,
                is_bot=sender.bot,
                is_scam=sender.scam,
                is_fake=sender.fake,
                is_premium=sender.premium,
                is_verified=sender.verified,
            )

    async def chat_processing_scheduler_loop(self):
        try:
            self.is_catching_up = True
            while True:
                async for dialog in self.client.iter_dialogs():
                    await self.database.insert_chat(dialog.id, dialog.title, dialog.is_group, dialog.is_channel, dialog.is_user)
                    await self.process_chat(dialog, fast_mode=self.is_catching_up)
                self.is_catching_up = False
                await asyncio.sleep(1800)
        except asyncio.CancelledError:
            pass

    async def process_chat(self, chat, fast_mode: bool = False):
        logging.info(f'Processing chat {chat.title}')

        total_number_of_messages = await self._get_total_number_of_messages(chat.id)
        database_number_of_messages = await self.database.get_total_number_of_messages(chat.id)
        process_number_of_messages = total_number_of_messages - database_number_of_messages
        message_id_cursor = await self.database.get_chat_message_id_cursor(chat.id) or 0

        with Progress(
            SpinnerColumn("dots"),
            *Progress.get_default_columns(),
            MessagesPerSecondColumn(),
            TextColumn("[progress.custom]{task.fields[cursor_position]}"),
        ) as progress:
            task = progress.add_task(f'Processing chat {chat.title}', total=process_number_of_messages, cursor_position=f"Cursor position: {None}")
            last_message = None
            wait_time = 1 if fast_mode else 2
            async for i, message in async_enumerate(self.client.iter_messages(chat, limit=None, reverse=True, min_id=message_id_cursor, wait_time=wait_time)):
                user_id = getattr(message.sender, 'id', None)
                await self.database.insert_message(message.id, user_id, message.chat_id, message.text, message.date)
                progress.advance(task)
                progress.update(task, cursor_position=f"Cursor position: {message.date}")
                if i % 1000 == 0:
                    await self.database.set_chat_message_id_cursor(message.chat_id, message.id)
                last_message = message
            if last_message:
                await self.database.set_chat_message_id_cursor(message.chat_id, message.id)

    async def _get_full_chat(self, channel):
        return await self.client(GetFullChannelRequest(channel))

    async def _get_total_number_of_messages(self, chat_id):
        return (await self.client.get_messages(chat_id, search='')).total

async def main():
    telegram_manager = TelegramManager()
    await telegram_manager.run()
    while True:
        await asyncio.sleep(1)

class MessagesPerSecondColumn(ProgressColumn):
    def render(self, task):
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("?", style="progress.data.speed")
        return Text(f"{int(speed)} msgs/s", style="progress.data.speed")

class ChatProcessingProgress(Progress):
    def __init__(self, *args, **kwargs):
        super().__init__(
            SpinnerColumn("dots"),
            *Progress.get_default_columns(),
            MessagesPerSecondColumn(),
            TextColumn("[progress.custom]{task.fields[cursor_position]}"),
        )

if __name__ == '__main__':
    asyncio.run(main())
