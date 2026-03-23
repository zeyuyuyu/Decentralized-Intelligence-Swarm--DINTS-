import asyncio
import random
from dataclasses import dataclass
from typing import List, Optional, Set

@dataclass
class NodeState:
    node_id: str
    is_leader: bool = False
    term: int = 0
    voted_for: Optional[str] = None
    known_peers: Set[str] = None

    def __post_init__(self):
        if self.known_peers is None:
            self.known_peers = set()

class SwarmNode:
    def __init__(self, node_id: str = None):
        self.node_id = node_id or hex(random.getrandbits(32))[2:]
        self.state = NodeState(node_id=self.node_id)
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = 0
        self.running = False

    async def start(self):
        """Start the swarm node and begin participating in the network"""
        self.running = True
        await asyncio.gather(
            self.run_election_timer(),
            self.heartbeat_loop()
        )

    async def run_election_timer(self):
        """Monitor election timeout and initiate leader election if needed"""
        while self.running:
            await asyncio.sleep(0.1)
            if (asyncio.get_event_loop().time() - self.last_heartbeat) > self.election_timeout:
                await self.start_election()

    async def start_election(self):
        """Initiate leader election process"""
        self.state.term += 1
        self.state.voted_for = self.node_id
        self.last_heartbeat = asyncio.get_event_loop().time()

        votes_received = 1  # Vote for self
        votes_needed = (len(self.state.known_peers) + 1) // 2 + 1

        # Request votes from all peers
        vote_futures = [self.request_vote(peer) for peer in self.state.known_peers]
        if vote_futures:
            votes = await asyncio.gather(*vote_futures)
            votes_received += sum(1 for v in votes if v)

        if votes_received >= votes_needed:
            self.state.is_leader = True
            print(f"Node {self.node_id} became leader for term {self.state.term}")

    async def request_vote(self, peer_id: str) -> bool:
        """Request vote from a peer node"""
        # In real implementation, this would make an RPC call
        # For now, simulate network delay and random voting
        await asyncio.sleep(random.uniform(0.1, 0.3))
        return random.random() > 0.3

    async def heartbeat_loop(self):
        """Send periodic heartbeats if leader"""
        while self.running:
            if self.state.is_leader:
                await self.send_heartbeat()
                await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(0.1)

    async def send_heartbeat(self):
        """Send heartbeat to all peers"""
        # In real implementation, this would make RPC calls
        # For now, just update local heartbeat time
        self.last_heartbeat = asyncio.get_event_loop().time()

    def add_peer(self, peer_id: str):
        """Add a new peer to the known peers set"""
        if peer_id != self.node_id:
            self.state.known_peers.add(peer_id)

    def remove_peer(self, peer_id: str):
        """Remove a peer from the known peers set"""
        self.state.known_peers.discard(peer_id)

    async def stop(self):
        """Stop the swarm node"""
        self.running = False
