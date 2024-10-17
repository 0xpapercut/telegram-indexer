import asyncio
import logging
import json
from dataclasses import dataclass
from datetime import datetime, timedelta

from telethon import TelegramClient, events

from rich.progress import Progress, TextColumn
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.console import Group
from rich.live import Live
from rich import print

from .database import DatabaseManager, UserRow, ChatRow, MessageRow, ChatParticipantsCountRow
from .websocket import WebSocketManager
from .utils import get_full_name
from .rich_utils import MessagesPerSecondColumn

class TelegramManager:

    def __init__(self, api_id, api_hash, database_manager: DatabaseManager, websocket_manager: WebSocketManager):
        self.client = TelegramClient('user', api_id, api_hash, connection_retries=None, request_retries=None)
        self.client.add_event_handler(self.new_message_handler, events.NewMessage)

        self.database = database_manager
        self.database_task = None

        self.websocket = websocket_manager
        self.websocket_task = None

        self.historical_messages_chat_queue = asyncio.Queue()
        self.historical_messages_scheduled_chat_set = set()
        self.historical_messages_loop_wait_time = 1

        self.dialogs_loop_wait_time = 20
        self.dialogs_loop_first_pass_event = asyncio.Event()
        self.dialogs_loop_last_pass_time = None

        self.scheduler_loop_wait_time = 60

        self.stop_event = asyncio.Event()

        self.main_logger = logging.getLogger('telegram_main')
        self.scheduler_logger = logging.getLogger('telegram_scheduler')
        self.historical_messages_logger = logging.getLogger('telegram_historical_messages')
        self.dialogs_logger = logging.getLogger('telegram_dialogs')

        self.historical_messages_current_chat_title = None
        self.historical_messages_current_chat_progress = None

        # self.console = Console()
        # self.panel = Panel()
        # self.group = Group()

    async def run(self):
        self.main_logger.info('Starting TelegramManager')
        self.websocket_task = asyncio.create_task(self.websocket.run())
        self.database_task = asyncio.create_task(self.database.run())
        await self.client.start()
        await self.main_loop()

    async def stop(self):
        self.main_logger.info('Stopping TelegramManager')
        self.stop_event.set()
        self.database.stop()
        self.websocket.stop()
        await asyncio.gather(
            self.client.disconnect(),
            self.database_task,
            self.websocket_task,
        )

    async def main_loop(self):
        historical_messages_task = asyncio.create_task(self.historical_messages_loop())
        dialogs_task = asyncio.create_task(self.dialogs_loop())
        scheduler_task = asyncio.create_task(self.scheduler_loop())

        # with Live(refresh_per_second=2) as live:
        #     while True:
        #         last_pass_timedelta = datetime.now() - self.dialogs_loop_last_pass_time if self.dialogs_loop_last_pass_time else None
        #         live.update(Text(f'Last dialogs pass: {last_pass_timedelta or '-'}'))
        #         await asyncio.sleep(1)

        await asyncio.gather(historical_messages_task, dialogs_task, scheduler_task)

    async def scheduler_loop(self):
        self.scheduler_logger.info('Waiting for iter dialogs first pass')
        await self.dialogs_loop_first_pass_event.wait()
        await asyncio.sleep(1) # Wait for one extra second before making queries on the database

        while True:
            chats_scheduled = 0
            self.scheduler_logger.info('Fetching messages from database for scheduling')
            rows = await self.database.get_all_latest_message_ids()
            for row in rows:
                if row['latest_historical_message_id'] is None or row['latest_message_id'] > row['latest_historical_message_id']:
                    if row['chat_id'] not in self.historical_messages_scheduled_chat_set:
                        chat_title = await self.database.get_chat_title(row['chat_id'])
                        # self.scheduler_logger.info(f'Inserting chat \'{chat_title}\' into processing queue')
                        self.historical_messages_scheduled_chat_set.add(row['chat_id'])
                        await self.historical_messages_chat_queue.put(row['chat_id'])
                        chats_scheduled += 1
            self.scheduler_logger.info(f'Scheduled {chats_scheduled} chats')
            self.scheduler_logger.info(f'Current queue size: {len(self.historical_messages_scheduled_chat_set)}')
            await asyncio.sleep(self.scheduler_loop_wait_time)

    async def dialogs_loop(self):
        while not self.stop_event.is_set():
            self.dialogs_logger.info('Looping through dialogs')
            async for dialog in self.client.iter_dialogs():
                if chat := await ChatRow.from_dialog(dialog):
                    await self.database.queue_insert(chat)
                if user := await UserRow.from_patched_message(dialog.message):
                    await self.database.queue_insert(user)
                if message := await MessageRow.from_patched_message(dialog.message):
                    await self.database.queue_insert(message)
                if chat_participants_count := await ChatParticipantsCountRow.from_dialog(dialog):
                    await self.database.queue_insert(chat_participants_count)
            self.dialogs_logger.info('Finished looping through dialogs')
            if not self.dialogs_loop_first_pass_event.is_set():
                self.dialogs_logger.info('Signaling first pass')
                self.dialogs_loop_first_pass_event.set()
            self.dialogs_loop_last_pass_time = datetime.now()
            print(f'Finished dialogs pass at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            await asyncio.sleep(self.dialogs_loop_wait_time)

    async def historical_messages_loop(self):
        while True:
            chat_id = await self.historical_messages_chat_queue.get()
            chat_title = await self.database.get_chat_title(chat_id)
            self.historical_messages_logger.info(f'Popped chat {chat_title} from processing queue')

            self.historical_messages_scheduled_chat_set.remove(chat_id)
            if row := await self.database.get_latest_historical_message(chat_id):
                min_id = row['message_id']
                self.historical_messages_logger.info(f'Processing historical information for chat \'{chat_title}\' from {row['date']}')
            else:
                min_id = 0
                self.historical_messages_logger.info(f'Processing historical information for chat \'{chat_title}\' from inception')

            wait_time = self.historical_messages_loop_wait_time

            total_messages = await self.get_total_number_of_messages(chat_id)
            remaining_messages = total_messages - await self.database.get_historical_message_count(chat_id)

            if remaining_messages == 0:
                continue

            chats_remaining = len(self.historical_messages_scheduled_chat_set)

            with Progress(*Progress.get_default_columns(), MessagesPerSecondColumn(), TextColumn(f'(Remaining chats: {chats_remaining})')) as progress:
                task = progress.add_task(f"[cyan]Processing chat '{chat_title}'", total=remaining_messages)
                async for message in self.client.iter_messages(chat_id, limit=None, reverse=True, min_id=min_id, wait_time=wait_time):
                    # We don't need to insert chat as it has already been inserted by iter_dialogs
                    if user := await UserRow.from_patched_message(message):
                        await self.database.queue_insert(user)
                    if message := await MessageRow.from_patched_message(message, is_historical=True):
                        await self.database.queue_insert(message)
                    progress.advance(task)

    async def new_message_handler(self, event: events.NewMessage.Event):
        if user := await UserRow.from_new_message_event(event):
            await self.database.queue_insert(user)
        if chat := await ChatRow.from_new_message_event(event):
            await self.database.queue_insert(chat)
        if message := await MessageRow.from_new_message_event(event):
            await self.database.queue_insert(message)

        serializable_message = await SerializableMessage.from_new_message_event(event)
        await self.websocket.broadcast(serializable_message.to_json())

    async def get_total_number_of_messages(self, chat_id):
        return (await self.client.get_messages(chat_id, search='')).total

@dataclass
class SerializableMessage:
    sender: str
    chat: str
    text: str

    @classmethod
    async def from_new_message_event(cls, event: events.NewMessage.Event):
        chat = await event.get_chat()
        sender = await event.message.get_sender()
        event.message.sender_id
        return cls(
            chat=getattr(chat, 'title', None) or get_full_name(chat),
            sender=getattr(sender, 'title', None) or get_full_name(sender),
            text=event.message.text,
        )

    def to_dict(self):
        return {
            'sender': self.sender,
            'chat': self.chat,
            'text': self.text,
        }

    def to_json(self):
        return json.dumps(self.to_dict())
