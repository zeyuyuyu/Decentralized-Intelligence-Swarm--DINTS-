import asyncio
import json
from typing import Dict, Set, Optional
import aiohttp
import logging

class SwarmNode:
    def __init__(self, node_id: str, host: str = 'localhost', port: int = 8000):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers: Dict[str, dict] = {}
        self.is_running = False
        self.heartbeat_interval = 30  # seconds
        self.logger = logging.getLogger('SwarmNode')

    async def start(self):
        """Start the swarm node and begin peer discovery"""
        self.is_running = True
        self.logger.info(f'Starting SwarmNode {self.node_id} at {self.host}:{self.port}')
        await asyncio.gather(
            self.run_discovery_server(),
            self.run_heartbeat()
        )

    async def run_discovery_server(self):
        """Run HTTP server for peer discovery and coordination"""
        async def handle_discovery(request):
            if request.method == 'POST':
                data = await request.json()
                peer_id = data.get('node_id')
                if peer_id and peer_id != self.node_id:
                    self.peers[peer_id] = {
                        'host': data.get('host'),
                        'port': data.get('port'),
                        'last_seen': asyncio.get_event_loop().time()
                    }
                    return aiohttp.web.Response(text=json.dumps({
                        'status': 'connected',
                        'node_id': self.node_id
                    }))
            return aiohttp.web.Response(text='Invalid request')

        app = aiohttp.web.Application()
        app.router.add_post('/discover', handle_discovery)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, self.host, self.port)
        await site.start()

    async def run_heartbeat(self):
        """Periodically check peer health and discover new peers"""
        while self.is_running:
            peers_to_remove = []
            current_time = asyncio.get_event_loop().time()

            # Check existing peers
            for peer_id, peer_info in self.peers.items():
                try:
                    async with aiohttp.ClientSession() as session:
                        url = f'http://{peer_info["host"]}:{peer_info["port"]}/discover'
                        async with session.post(url, json={
                            'node_id': self.node_id,
                            'host': self.host,
                            'port': self.port
                        }) as resp:
                            if resp.status != 200:
                                peers_to_remove.append(peer_id)
                            else:
                                self.peers[peer_id]['last_seen'] = current_time
                except Exception as e:
                    self.logger.warning(f'Failed to contact peer {peer_id}: {str(e)}')
                    peers_to_remove.append(peer_id)

            # Remove dead peers
            for peer_id in peers_to_remove:
                self.peers.pop(peer_id, None)

            # Cleanup old peers
            stale_time = current_time - (self.heartbeat_interval * 2)
            stale_peers = [pid for pid, pinfo in self.peers.items()
                          if pinfo['last_seen'] < stale_time]
            for peer_id in stale_peers:
                self.peers.pop(peer_id, None)

            await asyncio.sleep(self.heartbeat_interval)

    async def broadcast(self, message: dict):
        """Broadcast a message to all connected peers"""
        tasks = []
        for peer_id, peer_info in self.peers.items():
            tasks.append(self.send_to_peer(peer_id, message))
        await asyncio.gather(*tasks)

    async def send_to_peer(self, peer_id: str, message: dict):
        """Send a message to a specific peer"""
        if peer_id not in self.peers:
            return
        
        peer = self.peers[peer_id]
        try:
            async with aiohttp.ClientSession() as session:
                url = f'http://{peer["host"]}:{peer["port"]}/message'
                async with session.post(url, json=message) as resp:
                    return await resp.json()
        except Exception as e:
            self.logger.error(f'Failed to send message to peer {peer_id}: {str(e)}')

    async def stop(self):
        """Stop the swarm node"""
        self.is_running = False
        self.logger.info(f'Stopping SwarmNode {self.node_id}')
