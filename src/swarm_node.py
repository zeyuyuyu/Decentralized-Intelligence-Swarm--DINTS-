import asyncio
import json
from typing import Dict, Set, Optional
from dataclasses import dataclass
import aiohttp

@dataclass
class NodeInfo:
    node_id: str
    address: str
    capabilities: Set[str]
    last_seen: float

class SwarmNode:
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.address = f'http://{host}:{port}'
        self.peers: Dict[str, NodeInfo] = {}
        self.capabilities = {'compute', 'storage'}
        self.running = False

    async def start(self):
        self.running = True
        await asyncio.gather(
            self.discovery_heartbeat(),
            self.prune_stale_peers(),
            self.start_api_server()
        )

    async def discovery_heartbeat(self):
        """Regularly broadcast presence to known peers and discover new ones"""
        while self.running:
            async with aiohttp.ClientSession() as session:
                for peer in list(self.peers.values()):
                    try:
                        async with session.post(
                            f'{peer.address}/ping',
                            json={
                                'node_id': self.node_id,
                                'address': self.address,
                                'capabilities': list(self.capabilities)
                            }
                        ) as resp:
                            if resp.status == 200:
                                peer_data = await resp.json()
                                await self.handle_peer_discovery(peer_data)
                    except Exception:
                        pass
            await asyncio.sleep(5)

    async def handle_peer_discovery(self, peer_data: dict):
        """Process peer information and update mesh network topology"""
        peer_id = peer_data['node_id']
        if peer_id != self.node_id:
            self.peers[peer_id] = NodeInfo(
                node_id=peer_id,
                address=peer_data['address'],
                capabilities=set(peer_data['capabilities']),
                last_seen=asyncio.get_event_loop().time()
            )

    async def prune_stale_peers(self):
        """Remove peers that haven't been seen recently"""
        while self.running:
            current_time = asyncio.get_event_loop().time()
            stale_peers = [
                pid for pid, peer in self.peers.items()
                if current_time - peer.last_seen > 30
            ]
            for pid in stale_peers:
                del self.peers[pid]
            await asyncio.sleep(10)

    async def start_api_server(self):
        """Start HTTP API server for peer communication"""
        async def handler(request):
            if request.path == '/ping':
                peer_data = await request.json()
                await self.handle_peer_discovery(peer_data)
                return aiohttp.web.json_response({
                    'node_id': self.node_id,
                    'address': self.address,
                    'capabilities': list(self.capabilities)
                })

        app = aiohttp.web.Application()
        app.router.add_post('/ping', handler)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, self.host, self.port)
        await site.start()

    async def stop(self):
        """Gracefully shutdown the node"""
        self.running = False

    def get_peers_by_capability(self, capability: str) -> Set[NodeInfo]:
        """Find peers that have a specific capability"""
        return {p for p in self.peers.values() if capability in p.capabilities}

    async def broadcast_message(self, message: dict):
        """Send a message to all connected peers"""
        async with aiohttp.ClientSession() as session:
            tasks = []
            for peer in self.peers.values():
                task = session.post(
                    f'{peer.address}/message',
                    json=message
                )
                tasks.append(task)
            await asyncio.gather(*tasks, return_exceptions=True)
