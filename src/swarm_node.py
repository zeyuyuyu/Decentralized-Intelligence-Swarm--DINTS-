import asyncio
import json
import random
from typing import Dict, Set
from dataclasses import dataclass
from datetime import datetime

@dataclass
class NodeInfo:
    node_id: str
    last_seen: datetime
    capabilities: Set[str]
    address: str

class SwarmNode:
    def __init__(self, node_id: str, port: int):
        self.node_id = node_id
        self.port = port
        self.peers: Dict[str, NodeInfo] = {}
        self.capabilities = set(['compute', 'storage'])
        self.running = False

    async def start(self):
        self.running = True
        await asyncio.gather(
            self.discovery_broadcast(),
            self.listen_for_peers()
        )

    async def discovery_broadcast(self):
        """Periodically broadcast node presence to network"""
        while self.running:
            try:
                message = {
                    'type': 'discovery',
                    'node_id': self.node_id,
                    'capabilities': list(self.capabilities),
                    'timestamp': datetime.utcnow().isoformat()
                }
                # Broadcast to network
                await self._broadcast_message(json.dumps(message))
                await asyncio.sleep(random.uniform(5, 15))
            except Exception as e:
                print(f'Discovery broadcast error: {e}')
                await asyncio.sleep(5)

    async def listen_for_peers(self):
        """Listen for other peers on the network"""
        while self.running:
            try:
                reader, writer = await asyncio.start_server(
                    self._handle_peer_connection,
                    '0.0.0.0',
                    self.port
                )
                async with reader:
                    await reader.wait_closed()
            except Exception as e:
                print(f'Peer listening error: {e}')
                await asyncio.sleep(5)

    async def _handle_peer_connection(self, reader, writer):
        """Handle incoming peer connections"""
        try:
            data = await reader.read(4096)
            message = json.loads(data.decode())
            
            if message['type'] == 'discovery':
                peer_info = NodeInfo(
                    node_id=message['node_id'],
                    last_seen=datetime.fromisoformat(message['timestamp']),
                    capabilities=set(message['capabilities']),
                    address=writer.get_extra_info('peername')[0]
                )
                self.peers[peer_info.node_id] = peer_info
                
                # Send acknowledgment
                response = {
                    'type': 'discovery_ack',
                    'node_id': self.node_id
                }
                writer.write(json.dumps(response).encode())
                await writer.drain()
        except Exception as e:
            print(f'Error handling peer connection: {e}')
        finally:
            writer.close()
            await writer.wait_closed()

    async def _broadcast_message(self, message: str):
        """Broadcast message to all known peers"""
        for peer_id, peer_info in list(self.peers.items()):
            try:
                reader, writer = await asyncio.open_connection(
                    peer_info.address,
                    self.port
                )
                writer.write(message.encode())
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except Exception:
                # Remove unreachable peers
                del self.peers[peer_id]

    def stop(self):
        """Stop the node"""
        self.running = False

    def get_network_stats(self) -> dict:
        """Get statistics about the node's network"""
        return {
            'total_peers': len(self.peers),
            'peer_capabilities': {
                cap: sum(1 for p in self.peers.values() if cap in p.capabilities)
                for cap in {'compute', 'storage'}
            }
        }
