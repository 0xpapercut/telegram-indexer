import asyncpg
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

from telethon import events, types
from telethon.tl import patched

from .utils import get_username

class DatabaseManager:

    def __init__(self, dsn):
        self.dsn = dsn
        self.pool = None
        self._insert_queue = asyncio.Queue()
        self.batch_wait_time = 1
        self.stop_event = asyncio.Event()
        self.logger = logging.getLogger('database')

    async def run(self):
        self.pool = await asyncpg.create_pool(self.dsn)
        while True:
            await self.batch_insert_from_queue()
            await self.sleep()

    def stop(self):
        self.stop_event.set()

    async def get_latest_historical_message(self, chat_id):
        async with self.pool.acquire() as conn:
            try:
                row = await conn.fetchrow("""
                    SELECT
                        message_id, date
                    FROM messages
                    WHERE chat_id = $1 AND is_historical = true
                    ORDER BY message_id DESC
                    LIMIT 1
                """, chat_id)
                return row
            except asyncpg.PostgresError as e:
                self.logger.error(f'PostgreSQL error during query on chats table: {e}')

    async def get_all_latest_message_ids(self):
        async with self.pool.acquire() as conn:
            try:
                return await conn.fetch('''
                    SELECT
                        chat_id,
                        MAX(message_id) AS latest_message_id,
                        MAX(CASE WHEN is_historical THEN message_id ELSE NULL END) AS latest_historical_message_id
                    FROM messages
                    GROUP BY chat_id
                ''')
            except asyncpg.PostgresError as e:
                self.logger.error(f'PostgreSQL error during query on chats table: {e}')

    async def get_historical_message_count(self, chat_id):
        async with self.pool.acquire() as conn:
            try:
                row = await conn.fetchrow('''
                    SELECT COUNT(*) AS count
                    FROM messages
                    WHERE chat_id = $1 AND is_historical = true
                ''', chat_id)
                return row['count']
            except asyncpg.PostgresError as e:
                self.logger.error(f'PostgreSQL error during query on messages table: {e}')

    async def get_chat_title(self, chat_id):
        async with self.pool.acquire() as conn:
            try:
                row = await conn.fetchrow("""
                    SELECT title
                    FROM chats
                    WHERE chat_id = $1
                """, chat_id)
                return row['title']
            except asyncpg.PostgresError as e:
                self.logger.error(f'PostgreSQL error during query on chats table: {e}')

    async def sleep(self):
        await asyncio.sleep(self.batch_wait_time)
        # tasks = [asyncio.create_task(asyncio.sleep(delay)), asyncio.create_task(self.stop_event.wait())]
        # done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        # await done
        # for task in pending:
        #     task.cancel()

    async def queue_insert(self, item):
        if item is not None:
            await self._insert_queue.put(item)

    async def batch_insert_from_queue(self):
        users = []
        chats = []
        messages = []
        chats_participants_count = []
        while not self._insert_queue.empty():
            item =  await self._insert_queue.get()
            if isinstance(item, UserRow):
                users.append(item)
            elif isinstance(item, ChatRow):
                chats.append(item)
            elif isinstance(item, MessageRow):
                messages.append(item)
            elif isinstance(item, ChatParticipantsCountRow):
                chats_participants_count.append(item)
        if users:
            await self.batch_insert_users(users)
        if chats:
            await self.batch_insert_chats(chats)
        if messages:
            await self.batch_insert_messages(messages)
        if chats_participants_count:
            await self.batch_insert_chats_participants_count(chats_participants_count)

    async def batch_insert_users(self, users: list['UserRow']):
        async with self.pool.acquire() as conn:
            try:
                await conn.executemany("""
                    INSERT INTO users (user_id, username, first_name, last_name, is_bot, is_premium, is_scam, is_fake, is_verified)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (user_id) DO NOTHING
                """, [(row.user_id, row.username, row.first_name, row.last_name, row.is_bot, row.is_premium, row.is_scam, row.is_fake, row.is_verified) for row in users])
            except asyncpg.PostgresError as e:
                self.logger.error(f'PostgreSQL error during user insertion: {e}')

    async def batch_insert_chats(self, chats: list['ChatRow']):
        async with self.pool.acquire() as conn:
            try:
                await conn.executemany(f"""
                    INSERT INTO chats (chat_id, title, is_group, is_channel, is_user)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (chat_id) DO NOTHING
                """, [(row.chat_id, row.title, row.is_group, row.is_channel, row.is_user) for row in chats])
            except asyncpg.PostgresError as e:
                self.logger.error(f'PostgreSQL error during chat insertion: {e}')

    async def batch_insert_messages(self, messages: list['MessageRow']):
        async with self.pool.acquire() as conn:
            try:
                await conn.executemany("""
                    INSERT INTO messages (message_id, sender_id, chat_id, text, date, is_historical)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (chat_id, message_id) DO UPDATE SET is_historical = EXCLUDED.is_historical
                    WHERE messages.is_historical = false AND EXCLUDED.is_historical = true
                """, [(row.message_id, row.sender_id, row.chat_id, row.text, row.date, row.is_historical) for row in messages])
            except asyncpg.PostgresError as e:
                self.logger.error(f'PostgreSQL error during message insertion: {e}')

    async def batch_insert_chats_participants_count(self, chats_participants_count: list['ChatParticipantsCountRow']):
        async with self.pool.acquire() as conn:
            try:
                await conn.executemany("""
                    INSERT INTO chats_participants_count (chat_id, participants_count)
                    VALUES ($1, $2)
                """, [(row.chat_id, row.participants_count) for row in chats_participants_count])
            except asyncpg.PostgresError as e:
                self.logger.error(f'PostgreSQL error during chat_participants_count insertion: {e}')

@dataclass
class MessageRow:
    message_id: int
    sender_id: int
    chat_id: int
    text: str
    date: datetime
    is_historical: bool

    @classmethod
    async def from_new_message_event(cls, event: events.NewMessage.Event):
        if event.date:
            assert event.message.date is not None
        return await cls.from_patched_message(event.message)

    @classmethod
    async def from_patched_message(cls, message: patched.Message, is_historical=False):
        sender = await message.get_sender()
        if sender:
            return cls(
                chat_id=message.chat_id,
                message_id=message.id,
                sender_id=sender.id,
                text=message.text,
                date=message.date.replace(tzinfo=None),
                is_historical=is_historical,
            )

@dataclass
class UserRow:
    user_id: int
    username: str
    first_name: str
    last_name: str
    is_bot: bool
    is_premium: bool
    is_scam: bool
    is_fake: bool
    is_verified: bool

    @classmethod
    async def from_new_message_event(cls, event: events.NewMessage.Event):
        return await cls.from_patched_message(event.message)

    @classmethod
    async def from_patched_message(cls, message: patched.Message):
        sender = await message.get_sender()
        if isinstance(sender, types.User):
            return cls(
                user_id=sender.id,
                username=get_username(sender),
                first_name=sender.first_name,
                last_name=sender.last_name,
                is_bot=sender.bot,
                is_premium=sender.premium,
                is_scam=sender.scam,
                is_fake=sender.fake,
                is_verified=sender.verified,
            )
        else: # sender is Channel
            pass

@dataclass
class ChatRow:
    chat_id: int
    title: int
    is_group: bool
    is_channel: bool
    is_user: bool

    @classmethod
    async def from_new_message_event(cls, event: events.NewMessage.Event):
        chat = await event.get_chat()
        is_user = isinstance(chat, types.User)
        title = chat.title if not is_user else get_username(chat)
        return cls(
            chat_id=event.chat_id,
            title=title,
            is_group=event.is_group,
            is_channel=event.is_channel,
            is_user=is_user,
        )

    @classmethod
    async def from_dialog(cls, dialog: types.Dialog):
        title = dialog.entity.title if not dialog.is_user else get_username(dialog.entity)
        return cls(
            chat_id=dialog.id,
            title=title,
            is_group=dialog.is_group,
            is_channel=dialog.is_channel,
            is_user=dialog.is_user
        )

@dataclass
class ChatParticipantsCountRow:
    chat_id: int
    participants_count: int

    @classmethod
    async def from_dialog(cls, dialog: types.Dialog):
        if not dialog.is_user:
            return cls(
                chat_id=dialog.id,
                participants_count=dialog.entity.participants_count,
            )
