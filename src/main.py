import os
import json
import time
import random
from typing import List, Dict

from .swarm_node import SwarmNode
from .swarm_manager import SwarmManager

class DynamicSwarmScaler:
    def __init__(self, swarm_manager: SwarmManager):
        self.swarm_manager = swarm_manager
        self.min_nodes = int(os.getenv('MIN_SWARM_NODES', 3))
        self.max_nodes = int(os.getenv('MAX_SWARM_NODES', 10))
        self.target_cpu_utilization = float(os.getenv('TARGET_CPU_UTIL', 0.7))
        self.check_interval = int(os.getenv('SCALING_CHECK_INTERVAL', 60))
        self.last_scale_time = time.time()
        self.scale_cooldown = int(os.getenv('SCALING_COOLDOWN', 300))

    def run(self):
        while True:
            time.sleep(self.check_interval)
            self.check_and_scale()

    def check_and_scale(self):
        if time.time() - self.last_scale_time < self.scale_cooldown:
            return

        current_nodes = self.swarm_manager.get_all_nodes()
        current_cpu_util = self.calculate_average_cpu_util(current_nodes)

        if current_cpu_util > self.target_cpu_utilization and len(current_nodes) < self.max_nodes:
            self.scale_up()
        elif current_cpu_util < self.target_cpu_utilization and len(current_nodes) > self.min_nodes:
            self.scale_down()

    def scale_up(self):
        new_node = self.swarm_manager.add_node()
        self.last_scale_time = time.time()
        print(f'Scaled up, added new node: {new_node.id}')

    def scale_down(self):
        nodes_to_remove = self.swarm_manager.get_least_utilized_nodes(1)
        for node in nodes_to_remove:
            self.swarm_manager.remove_node(node)
        self.last_scale_time = time.time()
        print(f'Scaled down, removed node(s): {[n.id for n in nodes_to_remove]}')

    def calculate_average_cpu_util(self, nodes: List[SwarmNode]) -> float:
        total_util = sum(node.cpu_utilization for node in nodes)
        return total_util / len(nodes)
