import logging
from websockets import serve, broadcast

from rich.json import JSON
from rich import print

class WebSocketManager:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connections = set()
        self.server = None
        self.logger = logging.getLogger('websocket')

    async def run(self):
        self.server = await serve(self.handler, self.host, self.port)
        print(f'[bold green]Serving websocket at http://{self.host}:{self.port}[/bold green]')
        await self.server.serve_forever()

    async def stop(self):
        if self.serve is not None:
            await self.server.close()

    async def register(self, websocket):
        self.connections.add(websocket)
        self.logger(f"Client connected: {websocket}")

    async def unregister(self, websocket):
        self.connections.remove(websocket)
        self.logger(f"Client disconnected: {websocket}")

    async def broadcast(self, message: str):
        formatted_message = JSON(message, indent=None).text
        formatted_message.truncate(max_width=120)
        self.logger.info(f'Broadcasting message {formatted_message}')
        broadcast(self.connections, message)

    async def handler(self, websocket, path):
        await self.register(websocket)
        try:
            await websocket.wait_closed()
        finally:
            await self.unregister(websocket)
