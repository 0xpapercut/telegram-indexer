import os
import asyncio
import logging

from rich.logging import RichHandler

from .database import DatabaseManager
from .websocket import WebSocketManager
from .telegram import TelegramManager

API_ID = os.environ['API_ID']
API_HASH = os.environ['API_HASH']
DSN = os.environ['DSN']
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 5123

logging.basicConfig(
    # level=logging.WARN,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.StreamHandler(),
        logging.FileHandler('telegram_indexer.log'),
        RichHandler(logging.INFO),
    ]
)

async def main():
    database_manager = DatabaseManager(DSN)
    websocket_manager = WebSocketManager(DEFAULT_HOST, DEFAULT_PORT)
    telegram_manager = TelegramManager(API_ID, API_HASH, database_manager, websocket_manager)
    await telegram_manager.run()

if __name__ == '__main__':
    asyncio.run(main())
