import os
import aiosqlite
import asyncio
import datetime
import logging

class DatabaseManager:
    def __init__(self, path):
        self.lock = asyncio.Lock()
        self.path = path
        self.insert_queue = asyncio.Queue()
        self.stop_event = asyncio.Event()
        self.loop_task = None

    async def _create_tables(self):
        async with self.lock, aiosqlite.connect(self.path) as conn:
            with open('schema.sql', 'r') as schema_file:
                schema_sql = schema_file.read()
                await conn.executescript(schema_sql)
            await conn.commit()

    async def _loop(self):
        while not self.stop_event.is_set():
            await self._execute_insert_queue()
            await asyncio.sleep(1)
        await self._execute_insert_queue()

    async def get_total_number_of_messages(self, chat_id):
        async with self.lock, aiosqlite.connect(self.path) as conn:
            async with conn.execute('SELECT COUNT(*) FROM messages WHERE chat_id = ?', (chat_id,)) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else 0

    async def get_chat_message_id_cursor(self, chat_id):
        async with self.lock, aiosqlite.connect(self.path) as conn:
            async with conn.execute('SELECT message_id_cursor FROM chats WHERE chat_id = ?', (chat_id,)) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else None

    async def start(self):
        logging.info('Starting DatabaseManager')
        if not os.path.exists(os.path.dirname(self.path)):
            logging.info(f'{self.path} does not exist. Creating...')
            os.makedirs(os.path.dirname(self.path))
        if not os.path.exists(self.path):
            logging.info('No database detected. Creating...')
            await self._create_tables()
        else:
            logging.info('Database detected')
        self.loop_task = asyncio.create_task(self._loop())

    async def stop(self):
        self.stop_event.set()
        if self.loop_task:
            await self.loop_task

    async def insert_message(
        self,
        message_id,
        user_id,
        chat_id,
        text,
        date
    ):
        logging.info('Queueing insert to messages table')
        insertion_time = datetime.datetime.now()
        await self.insert_queue.put(('messages', (message_id, user_id, chat_id, text, date, insertion_time)))

    async def insert_user(
        self,
        user_id,
        username,
        first_name,
        last_name,
        is_bot,
        is_premium,
        is_scam,
        is_fake,
        is_verified,
    ):
        logging.info('Queueing insert to users table')
        await self.insert_queue.put(('users', (user_id, username, first_name, last_name, is_bot, is_premium, is_scam, is_fake, is_verified)))

    async def insert_chat(
        self,
        chat_id,
        title,
        is_group,
        is_channel,
        is_user,
        message_id_cursor=None
    ):
        logging.info('Queueing insert to chats table')
        await self.insert_queue.put(('chats', (chat_id, title, is_group, is_channel, is_user, message_id_cursor)))

    async def _execute_insert_queue(self):
        all_inserts = {}
        while not self.insert_queue.empty():
            table, values = await self.insert_queue.get()
            all_inserts.setdefault(table, []).append(values)

        async with self.lock, aiosqlite.connect(self.path) as conn:
            async with conn.cursor() as cursor:
                for key, all_values in all_inserts.items():
                    try:
                        if key == 'messages':
                            await cursor.executemany('INSERT OR IGNORE INTO messages VALUES (?, ?, ?, ?, ?, ?)', all_values)
                        elif key == 'users':
                            await cursor.executemany('INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', all_values)
                        elif key == 'chats':
                            await cursor.executemany('INSERT OR IGNORE INTO chats VALUES (?, ?, ?, ?, ?, ?)', all_values)
                        else:
                            raise ValueError("Unknown table")
                    except aiosqlite.ProgrammingError as e:
                        logging.error(f'SQLite error: {e}')
                await conn.commit()

    async def set_chat_message_id_cursor(self, chat_id, message_id):
        async with self.lock, aiosqlite.connect(self.path) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('''
                    INSERT INTO chats (chat_id, message_id_cursor)
                    VALUES (?, ?)
                    ON CONFLICT(chat_id) DO UPDATE SET
                    message_id_cursor=excluded.message_id_cursor
                ''', (chat_id, message_id))
                await conn.commit()
