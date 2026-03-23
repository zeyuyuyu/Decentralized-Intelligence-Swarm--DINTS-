import random
import time
import hashlib
import json

class SwarmNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.peers = []
        self.blockchain = []
        self.pending_transactions = []

    def connect_to_peer(self, peer_node):
        self.peers.append(peer_node)
        peer_node.peers.append(self)

    def broadcast_transaction(self, transaction):
        self.pending_transactions.append(transaction)
        for peer in self.peers:
            peer.receive_transaction(transaction)

    def receive_transaction(self, transaction):
        self.pending_transactions.append(transaction)

    def mine_block(self):
        if not self.pending_transactions:
            return

        new_block = {
            'index': len(self.blockchain) + 1,
            'timestamp': time.time(),
            'transactions': self.pending_transactions,
            'previous_hash': self.get_previous_hash()
        }

        new_block['hash'] = self.calculate_hash(new_block)
        self.blockchain.append(new_block)
        self.pending_transactions = []

        for peer in self.peers:
            peer.receive_new_block(new_block)

    def get_previous_hash(self):
        if not self.blockchain:
            return '0'
        return self.blockchain[-1]['hash']

    def calculate_hash(self, block):
        block_string = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(block_string).hexdigest()

    def receive_new_block(self, new_block):
        if self.is_valid_block(new_block):
            self.blockchain.append(new_block)
            for transaction in new_block['transactions']:
                if transaction in self.pending_transactions:
                    self.pending_transactions.remove(transaction)

    def is_valid_block(self, block):
        if block['index'] != len(self.blockchain) + 1:
            return False

        if block['previous_hash'] != self.get_previous_hash():
            return False

        if block['hash'] != self.calculate_hash(block):
            return False

        return True

    def reach_consensus(self):
        max_length = len(self.blockchain)
        for peer in self.peers:
            if len(peer.blockchain) > max_length:
                self.blockchain = peer.blockchain
                max_length = len(self.blockchain)
