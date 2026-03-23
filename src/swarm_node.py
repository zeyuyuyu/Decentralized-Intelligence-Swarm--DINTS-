import numpy as np
from typing import List

class SwarmNode:
    def __init__(self, node_id: str, initial_model: np.ndarray):
        self.node_id = node_id
        self.local_model = initial_model
        self.neighbors: List[SwarmNode] = []
        self.aggregated_model = None

    def add_neighbor(self, neighbor: 'SwarmNode'):
        self.neighbors.append(neighbor)

    def update_local_model(self, new_model: np.ndarray):
        self.local_model = new_model

    def aggregate_models(self):
        if not self.neighbors:
            self.aggregated_model = self.local_model
            return

        total_weight = 1.0 + len(self.neighbors)
        weighted_sum = self.local_model * 1.0
        for neighbor in self.neighbors:
            weighted_sum += neighbor.local_model
        self.aggregated_model = weighted_sum / total_weight

    def learn_from_aggregated_model(self):
        if self.aggregated_model is None:
            return
        self.local_model = self.aggregated_model
