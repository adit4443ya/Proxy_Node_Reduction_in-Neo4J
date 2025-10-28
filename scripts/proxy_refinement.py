import pickle
from collections import defaultdict

class ProxyAwareRefinement:
    def __init__(self, graph, partition_assignment, edge_weights=None, balance_tolerance=0.05, max_moves_per_iter=100):
        self.graph = graph
        self.partition = partition_assignment.copy()
        self.edge_weights = edge_weights or {}
        self.balance_tolerance = balance_tolerance
        self.num_partitions = max(partition_assignment.values()) + 1
        self.max_moves_per_iter = max_moves_per_iter
    
    def compute_partitions_connected(self, node):
        node_partition = self.partition[node]
        connected = set()
        for neighbor in self.graph.neighbors(node):
            p = self.partition[neighbor]
            if p != node_partition:
                connected.add(p)
        return len(connected)
    
    def compute_migration_gain(self, node, target_partition):
        current_partition = self.partition[node]
        if current_partition == target_partition:
            return 0
        partition_neighbors = defaultdict(float)
        for neighbor in self.graph.neighbors(node):
            part = self.partition[neighbor]
            w = self.edge_weights.get((node, neighbor), self.edge_weights.get((neighbor, node), 1))
            partition_neighbors[part] += w
        
        current_proxies = len([p for p in partition_neighbors if p != current_partition])
        new_proxies = len([p for p in partition_neighbors if p != target_partition])
        gain = current_proxies - new_proxies
        
        partition_sizes = self.get_partition_sizes()
        avg_size = len(self.graph.nodes()) / self.num_partitions
        new_sizes = partition_sizes.copy()
        new_sizes[current_partition] -= 1
        new_sizes[target_partition] += 1
        
        penalty = 0
        for size in new_sizes.values():
            if abs(size - avg_size) / avg_size > self.balance_tolerance:
                penalty = 10
        return gain - penalty
    
    def get_partition_sizes(self):
        sizes = defaultdict(int)
        for node, part in self.partition.items():
            sizes[part] += 1
        return sizes
    
    def refine(self, max_iters=10):
        print("Starting proxy-aware refinement...")
        for it in range(max_iters):
            print(f"Iteration {it+1}/{max_iters}")
            bridge_nodes = [(n, self.compute_partitions_connected(n)) for n in self.graph.nodes() if self.compute_partitions_connected(n) > 0]
            bridge_nodes.sort(key=lambda x: -x[1])
            moves = 0
            for node, _ in bridge_nodes:
                current = self.partition[node]
                best_part = current
                best_gain = -float('inf')
                for target in range(self.num_partitions):
                    gain = self.compute_migration_gain(node, target)
                    if gain > best_gain:
                        best_gain = gain
                        best_part = target
                if best_part != current and best_gain > 0:
                    self.partition[node] = best_part
                    moves += 1
                if moves >= self.max_moves_per_iter:
                    break
            print(f"Moves made: {moves}")
            if moves == 0:
                print("No moves made; converged")
                break
        return self.partition


if __name__ == "__main__":
    with open('../data/graph.pkl', 'rb') as f:
        graph = pickle.load(f)['graph']
    with open('../data/metis_partition_weighted.pkl', 'rb') as f:
        partition_data = pickle.load(f)
        partition_assignment = partition_data['partition_assignment']
    
    edge_weights = {}  # Load or simulate your workload edge weights here
    
    refiner = ProxyAwareRefinement(graph, partition_assignment, edge_weights=edge_weights)
    refined_partition = refiner.refine(max_iters=10)
    
    from partition_graph import GraphPartitioner
    partitioner = GraphPartitioner(graph, num_partitions=3, edge_weights=edge_weights)
    print("\n=== AFTER REFINEMENT ===")
    refined_stats = partitioner.analyze_partition(refined_partition)
    
    with open('../data/refined_partition_weighted.pkl', 'wb') as f:
        pickle.dump({
            'partition_assignment': refined_partition,
            'stats': refined_stats
        }, f)
