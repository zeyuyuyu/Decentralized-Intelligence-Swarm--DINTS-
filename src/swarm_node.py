import asyncio
import json
import random
from typing import Dict, List, Set
from dataclasses import dataclass
from datetime import datetime

@dataclass
class PeerInfo:
    id: str
    address: str
    last_seen: datetime
    capabilities: List[str]

class SwarmNode:
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers: Dict[str, PeerInfo] = {}
        self.active_connections: Set[str] = set()
        self.capabilities = ['compute', 'storage', 'network']

    async def start(self):
        server = await asyncio.start_server(
            self._handle_connection, self.host, self.port
        )
        await self._start_discovery()
        async with server:
            await server.serve_forever()

    async def _start_discovery(self):
        while True:
            self._cleanup_stale_peers()
            await self._broadcast_presence()
            await asyncio.sleep(30)

    async def _broadcast_presence(self):
        announcement = {
            'type': 'discovery',
            'node_id': self.node_id,
            'capabilities': self.capabilities,
            'timestamp': datetime.utcnow().isoformat()
        }
        for peer_id, peer in self.peers.items():
            try:
                reader, writer = await asyncio.open_connection(
                    *peer.address.split(':')
                )
                writer.write(json.dumps(announcement).encode())
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except:
                self.active_connections.discard(peer_id)

    def _cleanup_stale_peers(self, max_age_seconds: int = 300):
        now = datetime.utcnow()
        stale_peers = [
            pid for pid, p in self.peers.items()
            if (now - p.last_seen).total_seconds() > max_age_seconds
        ]
        for pid in stale_peers:
            del self.peers[pid]
            self.active_connections.discard(pid)

    async def _handle_connection(self, reader, writer):
        try:
            data = await reader.read(8192)
            message = json.loads(data.decode())
            
            if message['type'] == 'discovery':
                peer_id = message['node_id']
                addr = writer.get_extra_info('peername')
                self.peers[peer_id] = PeerInfo(
                    id=peer_id,
                    address=f'{addr[0]}:{addr[1]}',
                    last_seen=datetime.utcnow(),
                    capabilities=message['capabilities']
                )
                self.active_connections.add(peer_id)

            writer.close()
            await writer.wait_closed()

        except Exception as e:
            print(f'Error handling connection: {e}')

    async def find_peers_with_capability(self, capability: str) -> List[PeerInfo]:
        return [
            peer for peer in self.peers.values()
            if capability in peer.capabilities
        ]

    async def get_network_stats(self) -> Dict:
        return {
            'total_peers': len(self.peers),
            'active_connections': len(self.active_connections),
            'capabilities_distribution': self._get_capability_stats()
        }
    
    def _get_capability_stats(self) -> Dict[str, int]:
        stats = {}
        for peer in self.peers.values():
            for cap in peer.capabilities:
                stats[cap] = stats.get(cap, 0) + 1
        return stats