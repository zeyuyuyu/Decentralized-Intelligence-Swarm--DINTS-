import asyncio
import json
from typing import Dict, Set
from dataclasses import dataclass
from cryptography.fernet import Fernet

@dataclass
class SwarmNode:
    node_id: str
    host: str 
    port: int
    peers: Set[str] = None
    
    def __post_init__(self):
        self.peers = set()
        self.cipher_suite = Fernet(Fernet.generate_key())
        
    async def start(self):
        """Start the node's server and discovery service"""
        server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )
        await self.discover_peers()
        async with server:
            await server.serve_forever()
            
    async def handle_connection(self, reader, writer):
        """Handle incoming peer connections"""
        data = await reader.read(1024)
        message = json.loads(self.cipher_suite.decrypt(data))
        
        if message['type'] == 'discovery':
            self.peers.add(message['node_id'])
            response = {
                'type': 'discovery_ack',
                'node_id': self.node_id,
                'peers': list(self.peers)
            }
            writer.write(self.cipher_suite.encrypt(json.dumps(response).encode()))
            await writer.drain()
            
        writer.close()
        await writer.wait_closed()
        
    async def discover_peers(self):
        """Actively discover other nodes in the network"""
        for port in range(8000, 9000):
            if port != self.port:
                try:
                    reader, writer = await asyncio.open_connection(
                        self.host, port
                    )
                    
                    message = {
                        'type': 'discovery',
                        'node_id': self.node_id
                    }
                    writer.write(self.cipher_suite.encrypt(json.dumps(message).encode()))
                    await writer.drain()
                    
                    data = await reader.read(1024)
                    response = json.loads(self.cipher_suite.decrypt(data))
                    
                    if response['type'] == 'discovery_ack':
                        self.peers.add(response['node_id'])
                        self.peers.update(response['peers'])
                        
                    writer.close()
                    await writer.wait_closed()
                    
                except:
                    continue
                    
    async def broadcast(self, message: Dict):
        """Broadcast a message to all known peers"""
        for peer in self.peers:
            try:
                reader, writer = await asyncio.open_connection(
                    self.host, int(peer.split(':')[1])
                )
                writer.write(self.cipher_suite.encrypt(json.dumps(message).encode()))
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except:
                self.peers.remove(peer)
                continue

def create_node(host: str, port: int) -> SwarmNode:
    """Factory function to create a new SwarmNode"""
    node_id = f"{host}:{port}"
    return SwarmNode(node_id=node_id, host=host, port=port)