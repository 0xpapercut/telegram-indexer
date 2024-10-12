from websockets.asyncio.server import broadcast, serve, ServerConnection

class Server:

    def __init__(self):
        self.server = None

    async def handler(self, websocket: ServerConnection , path):
        print('New client connected to server')
        try:
            await websocket.wait_closed()
        finally:
            print('Client disconnected from server')

    async def broadcast_message(self, message):
        print(f'Relaying message {message.id} to all clients')
        broadcast(self.server.connections, message)

    async def serve(self, host, port):
        print('Starting server...')
        self.server = await serve(self.handler, host, port)
        await self.server.wait_closed()

    async def close(self):
        self.server.close()
