import os
import asyncio
import logging

from rich.progress import Progress, SpinnerColumn, ProgressColumn, TextColumn
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.text import Text

from .database import DatabaseManager
from .websocket import WebSocketManager
from .telegram import TelegramManager
from .utils import async_enumerate

API_ID = os.environ['API_ID']
API_HASH = os.environ['API_HASH']
DSN = os.environ['DSN']
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 5123

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('telegram_indexer.log')
    ]
)

async def main():
    database_manager = DatabaseManager(DSN)
    websocket_manager = WebSocketManager(DEFAULT_HOST, DEFAULT_PORT)
    telegram_manager = TelegramManager(API_ID, API_HASH, database_manager, websocket_manager)
    await telegram_manager.run()

if __name__ == '__main__':
    asyncio.run(main())
