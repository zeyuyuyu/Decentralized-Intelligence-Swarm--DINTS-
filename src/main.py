import numpy as np
from swarm_node import SwarmNode

class SwarmOptimizer:
    def __init__(self, num_nodes, objective_function, bounds):
        self.num_nodes = num_nodes
        self.objective_function = objective_function
        self.bounds = bounds
        self.nodes = [SwarmNode(self.objective_function, self.bounds) for _ in range(self.num_nodes)]
        self.global_best = None
        self.global_best_score = float('inf')

    def run(self, max_iterations, tolerance):
        for i in range(max_iterations):
            # Broadcast global best to all nodes
            for node in self.nodes:
                node.set_global_best(self.global_best)

            # Update node positions and velocities
            for node in self.nodes:
                node.update()

            # Evaluate objective function for each node
            scores = [node.evaluate() for node in self.nodes]

            # Update global best
            for score, node in zip(scores, self.nodes):
                if score < self.global_best_score:
                    self.global_best = node.position
                    self.global_best_score = score

            # Check for convergence
            if self.global_best_score < tolerance:
                break

        return self.global_best

if __name__ == '__main__':
    objective_function = lambda x: np.sum(x ** 2)
    bounds = [(-10, 10), (-10, 10), (-10, 10)]
    optimizer = SwarmOptimizer(10, objective_function, bounds)
    result = optimizer.run(100, 1e-6)
    print(f'Optimal solution: {result}')