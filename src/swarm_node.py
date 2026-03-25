import asyncio
import json
from typing import Dict, List, Set
import websockets
import random

class SwarmNode:
    def __init__(self, node_id: str, host: str = 'localhost', port: int = 8765):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers: Dict[str, str] = {}  # node_id -> websocket_uri
        self.active_connections: Set[str] = set()
        self.message_handlers = []

    async def start(self):
        """Start the node's server and initiate peer discovery"""
        self.server = await websockets.serve(
            self.handle_connection,
            self.host,
            self.port
        )
        print(f'Node {self.node_id} listening on {self.host}:{self.port}')
        await self.start_peer_discovery()

    async def handle_connection(self, websocket, path):
        """Handle incoming peer connections"""
        try:
            async for message in websocket:
                data = json.loads(message)
                if data['type'] == 'discover':
                    await self.handle_discovery(websocket, data)
                elif data['type'] == 'message':
                    await self.broadcast_message(data['content'], exclude=data.get('from'))
                    for handler in self.message_handlers:
                        await handler(data['content'])
        except websockets.exceptions.ConnectionClosed:
            peer_id = next(k for k, v in self.peers.items() if v == websocket)
            self.active_connections.remove(peer_id)

    async def handle_discovery(self, websocket, data):
        """Process peer discovery messages"""
        peer_id = data['node_id']
        peer_uri = data['uri']
        self.peers[peer_id] = peer_uri
        self.active_connections.add(peer_id)
        
        # Share known peers
        await websocket.send(json.dumps({
            'type': 'peers',
            'peers': self.peers
        }))

    async def connect_to_peer(self, peer_uri: str):
        """Establish connection to a new peer"""
        try:
            async with websockets.connect(peer_uri) as websocket:
                await websocket.send(json.dumps({
                    'type': 'discover',
                    'node_id': self.node_id,
                    'uri': f'ws://{self.host}:{self.port}'
                }))
                
                async for message in websocket:
                    data = json.loads(message)
                    if data['type'] == 'peers':
                        for peer_id, uri in data['peers'].items():
                            if peer_id not in self.peers and peer_id != self.node_id:
                                self.peers[peer_id] = uri
                                await self.connect_to_peer(uri)
        except:
            print(f'Failed to connect to peer: {peer_uri}')

    async def start_peer_discovery(self):
        """Begin periodic peer discovery process"""
        while True:
            if len(self.peers) < 5:  # Maintain minimum peer connections
                if self.peers:
                    # Connect to random known peer
                    peer_id = random.choice(list(self.peers.keys()))
                    if peer_id not in self.active_connections:
                        await self.connect_to_peer(self.peers[peer_id])
            await asyncio.sleep(5)  # Check every 5 seconds

    async def broadcast_message(self, message: str, exclude: str = None):
        """Broadcast message to all connected peers"""
        message_data = json.dumps({
            'type': 'message',
            'from': self.node_id,
            'content': message
        })
        
        for peer_id, uri in self.peers.items():
            if peer_id != exclude and peer_id in self.active_connections:
                try:
                    async with websockets.connect(uri) as websocket:
                        await websocket.send(message_data)
                except:
                    self.active_connections.remove(peer_id)

    def on_message(self, handler):
        """Register a message handler"""
        self.message_handlers.append(handler)
