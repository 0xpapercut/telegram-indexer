import asyncio
import logging
import json
from dataclasses import dataclass
from datetime import datetime, timedelta

from telethon import TelegramClient, events, types
from rich.progress_bar import ProgressBar
from rich.progress import Progress

from .database import DatabaseManager, UserRow, ChatRow, MessageRow, ChatParticipantsCountRow
from .websocket import WebSocketManager
from .utils import get_full_name

class TelegramManager:

    def __init__(self, api_id, api_hash, database_manager: DatabaseManager, websocket_manager: WebSocketManager):
        self.client = TelegramClient('user', api_id, api_hash)
        self.client.add_event_handler(self.new_message_handler, events.NewMessage)

        self.database = database_manager
        self.database_task = None

        self.websocket = websocket_manager
        self.websocket_task = None

        self.get_chat_historical_messages_queue = asyncio.Queue()
        self.get_chat_historical_message_schedule_set = set()
        self.get_chat_historical_messages_wait_time = 1
        self.iter_dialogs_loop_wait_time = 20

        self.stop_event = asyncio.Event()

    async def run(self):
        logging.info('Starting TelegramManager')
        self.websocket_task = asyncio.create_task(self.websocket.run())
        self.database_task = asyncio.create_task(self.database.run())
        await self.client.start()
        await self.main_loop()

    async def stop(self):
        logging.info('Stopping TelegramManager')
        self.stop_event.set()
        self.database.stop()
        self.websocket.stop()
        await asyncio.gather(
            self.client.disconnect(),
            self.database_task,
            self.websocket_task,
        )

    async def main_loop(self):
        historical_messages_task = asyncio.create_task(self.get_chat_historical_messages_loop())
        dialog_task = asyncio.create_task(self.iter_dialogs_loop())

        while True:
            rows = await self.database.get_all_latest_message_ids()
            for row in rows:
                if row['latest_historical_message_id'] is None or row['latest_message_id'] > row['latest_historical_message_id']:
                    self.get_chat_historical_message_schedule_set.add(row['chat_id'])
                    await self.get_chat_historical_messages_queue.put(row['chat_id'])
            await asyncio.sleep(60)

        await asyncio.gather(historical_messages_task, dialog_task)

    async def iter_dialogs_loop(self):
        while not self.stop_event.is_set():
            logging.info('Looping through dialogs')
            async for dialog in self.client.iter_dialogs():
                if chat := await ChatRow.from_dialog(dialog):
                    await self.database.queue_insert(chat)
                # logging.info(f'Adding {chat} to chats')
                if user := await UserRow.from_patched_message(dialog.message):
                    await self.database.queue_insert(user)
                if message := await MessageRow.from_patched_message(dialog.message):
                    await self.database.queue_insert(message)
                if chat_participants_count := await ChatParticipantsCountRow.from_dialog(dialog):
                    await self.database.queue_insert(chat_participants_count)
            await asyncio.sleep(self.iter_dialogs_loop_wait_time)

    async def get_chat_historical_messages_loop(self):
        while True:
            chat_id = await self.get_chat_historical_messages_queue.get()
            self.get_chat_historical_message_schedule_set.remove(chat_id)
            logging.info(f'Processing historical information for chat {chat_id}')
            min_id = await self.database.get_latest_historical_message_id(chat_id) or 0
            wait_time = self.get_chat_historical_messages_wait_time
            total_messages = await self.get_total_number_of_messages(chat_id)
            remaining_messages = total_messages - await self.database.get_historical_message_count(chat_id)
            with Progress() as progress:
                task = progress.add_task(f"[cyan]Processing chat '{await self.database.get_chat_title(chat_id)}'", total=remaining_messages)
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

    # async def add_handle_to_historical_messages_loop(handle):
    #     pass
        # def handle(chat_title, total_messages, processed_messages)

    # async def process_dialog(self, dialog):
    #     pass

    # async def process_patched_message(self, message):
    #     pass

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
