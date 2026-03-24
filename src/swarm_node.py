import asyncio
import json
import time
from dataclasses import dataclass
from typing import Dict, Set, Optional

@dataclass
class PeerInfo:
    address: str
    last_seen: float
    is_active: bool

class SwarmNode:
    def __init__(self, host: str, port: int, node_id: str):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.peers: Dict[str, PeerInfo] = {}
        self.is_running = False
        self.heartbeat_interval = 5.0  # seconds

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )
        self.is_running = True
        asyncio.create_task(self.heartbeat_loop())
        print(f'Node {self.node_id} listening on {self.host}:{self.port}')

    async def stop(self):
        self.is_running = False
        self.server.close()
        await self.server.wait_closed()

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            data = await reader.read(1024)
            message = json.loads(data.decode())
            
            if message['type'] == 'discovery':
                await self.handle_discovery(message, writer)
            elif message['type'] == 'heartbeat':
                await self.handle_heartbeat(message)

            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f'Error handling connection: {e}')

    async def handle_discovery(self, message: dict, writer: asyncio.StreamWriter):
        peer_id = message['node_id']
        peer_addr = message['address']
        
        if peer_id not in self.peers:
            self.peers[peer_id] = PeerInfo(
                address=peer_addr,
                last_seen=time.time(),
                is_active=True
            )
            print(f'New peer discovered: {peer_id} at {peer_addr}')

        # Send back our peer list
        response = {
            'type': 'discovery_response',
            'peers': [
                {'id': pid, 'address': p.address}
                for pid, p in self.peers.items()
                if p.is_active
            ]
        }
        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def handle_heartbeat(self, message: dict):
        peer_id = message['node_id']
        if peer_id in self.peers:
            self.peers[peer_id].last_seen = time.time()
            self.peers[peer_id].is_active = True

    async def heartbeat_loop(self):
        while self.is_running:
            current_time = time.time()
            
            # Check for inactive peers
            for peer_id, peer in self.peers.items():
                if current_time - peer.last_seen > self.heartbeat_interval * 3:
                    peer.is_active = False
                    print(f'Peer {peer_id} became inactive')

            # Send heartbeat to active peers
            for peer_id, peer in self.peers.items():
                if peer.is_active:
                    try:
                        reader, writer = await asyncio.open_connection(
                            *peer.address.split(':')
                        )
                        message = {
                            'type': 'heartbeat',
                            'node_id': self.node_id
                        }
                        writer.write(json.dumps(message).encode())
                        await writer.drain()
                        writer.close()
                        await writer.wait_closed()
                    except Exception as e:
                        print(f'Failed to send heartbeat to {peer_id}: {e}')
                        peer.is_active = False

            await asyncio.sleep(self.heartbeat_interval)

    async def discover_peers(self, bootstrap_nodes: Set[str]):
        for addr in bootstrap_nodes:
            try:
                reader, writer = await asyncio.open_connection(
                    *addr.split(':')
                )
                message = {
                    'type': 'discovery',
                    'node_id': self.node_id,
                    'address': f'{self.host}:{self.port}'
                }
                writer.write(json.dumps(message).encode())
                await writer.drain()

                data = await reader.read(1024)
                response = json.loads(data.decode())

                if response['type'] == 'discovery_response':
                    for peer in response['peers']:
                        if peer['id'] not in self.peers and peer['id'] != self.node_id:
                            self.peers[peer['id']] = PeerInfo(
                                address=peer['address'],
                                last_seen=time.time(),
                                is_active=True
                            )

                writer.close()
                await writer.wait_closed()
            except Exception as e:
                print(f'Failed to discover peers from {addr}: {e}')
