import asyncio
import json
from typing import Dict, List, Set
import websockets
import uuid

class SwarmNode:
    def __init__(self, host: str = '0.0.0.0', port: int = 8765):
        self.host = host
        self.port = port
        self.node_id = str(uuid.uuid4())
        self.peers: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.known_nodes: Set[str] = set()
        self.is_running = False

    async def start(self):
        self.is_running = True
        self.server = await websockets.serve(
            self.handle_connection,
            self.host,
            self.port
        )
        print(f'Node {self.node_id} listening on {self.host}:{self.port}')
        await self.server.wait_closed()

    async def handle_connection(self, websocket: websockets.WebSocketServerProtocol, path: str):
        try:
            # Handle initial handshake
            msg = await websocket.recv()
            data = json.loads(msg)
            
            if data['type'] == 'join':
                peer_id = data['node_id']
                self.peers[peer_id] = websocket
                self.known_nodes.add(peer_id)
                
                # Share known peers
                await self.broadcast_peers()
                
                # Listen for messages
                async for message in websocket:
                    await self.handle_message(peer_id, message)
                    
        except websockets.exceptions.ConnectionClosed:
            if peer_id in self.peers:
                del self.peers[peer_id]
                self.known_nodes.remove(peer_id)
                await self.broadcast_peers()

    async def connect_to_peer(self, peer_host: str, peer_port: int):
        uri = f'ws://{peer_host}:{peer_port}'
        try:
            async with websockets.connect(uri) as websocket:
                # Send join message
                join_msg = {
                    'type': 'join',
                    'node_id': self.node_id
                }
                await websocket.send(json.dumps(join_msg))
                
                # Handle responses
                async for message in websocket:
                    await self.handle_message(None, message)
                    
        except Exception as e:
            print(f'Failed to connect to peer {uri}: {str(e)}')

    async def broadcast_peers(self):
        peer_list = list(self.known_nodes)
        msg = {
            'type': 'peers',
            'nodes': peer_list
        }
        await self.broadcast(json.dumps(msg))

    async def broadcast(self, message: str):
        disconnected_peers = []
        for peer_id, websocket in self.peers.items():
            try:
                await websocket.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_peers.append(peer_id)
        
        # Clean up disconnected peers
        for peer_id in disconnected_peers:
            del self.peers[peer_id]
            self.known_nodes.remove(peer_id)

    async def handle_message(self, sender_id: str, message: str):
        try:
            data = json.loads(message)
            if data['type'] == 'peers':
                new_nodes = set(data['nodes']) - self.known_nodes
                self.known_nodes.update(new_nodes)
                # Could trigger connection to new nodes here
            # Add other message type handlers here
            
        except json.JSONDecodeError:
            print(f'Invalid message format from {sender_id}')

    async def stop(self):
        self.is_running = False
        if hasattr(self, 'server'):
            self.server.close()
            await self.server.wait_closed()
        
        # Close all peer connections
        for websocket in self.peers.values():
            await websocket.close()
        self.peers.clear()
        self.known_nodes.clear()