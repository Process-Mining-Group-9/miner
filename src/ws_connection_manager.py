from typing import List, Tuple
from fastapi import WebSocket
import logging


class ConnectionManager:
    """Handles multiple WebSocket connections at once.
    From documentation: https://fastapi.tiangolo.com/advanced/websockets/"""
    def __init__(self):
        self.active_connections: List[Tuple[WebSocket, str]] = []

    async def connect(self, websocket: WebSocket, log: str):
        await websocket.accept()
        self.active_connections.append((websocket, log))

    def disconnect(self, websocket: WebSocket):
        self.active_connections = [x for x in self.active_connections if x[0] is not websocket]

    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Send a message to a single connection."""
        await websocket.send_text(message)

    async def broadcast(self, message: str, log: str) -> int:
        """Broadcast a message to all connections listening to a log."""
        applicable = [x for x in self.active_connections if x[1] == log]
        for connection in applicable:
            try:
                await connection[0].send_text(message)
            except Exception as e:
                logging.error(f'Error while attempting to broadcast message to {connection}: {e}')
        return len(applicable)
