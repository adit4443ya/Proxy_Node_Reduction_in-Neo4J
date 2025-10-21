import pickle
import networkx as nx
from collections import defaultdict
import numpy as np

class ProxyAwareRefinement:
    def __init__(self, graph, partition_assignment, balance_tolerance=0.05):
        self.graph = graph
        self.partition = partition_assignment.copy()
        self.balance_tolerance = balance_tolerance
        self.num_partitions = max(partition_assignment.values()) + 1
        
    def compute_cross_partition_degree(self, node):
        """Compute number of edges crossing partitions"""
        node_partition = self.partition[node]
        cross_edges = 0
        
        for neighbor in self.graph.neighbors(node):
            if self.partition[neighbor] != node_partition:
                cross_edges += 1
        
        return cross_edges
    
    def compute_partitions_connected(self, node):
        """Count distinct partitions this node connects to"""
        node_partition = self.partition[node]
        connected_partitions = set()
        
        for neighbor in self.graph.neighbors(node):
            neighbor_partition = self.partition[neighbor]
            if neighbor_partition != node_partition:
                connected_partitions.add(neighbor_partition)
        
        return len(connected_partitions)
    
    def compute_migration_gain(self, node, target_partition):
        """Compute gain from moving node to target partition"""
        current_partition = self.partition[node]
        
        if current_partition == target_partition:
            return 0
        
        # Count neighbors in each partition
        partition_neighbors = defaultdict(int)
        for neighbor in self.graph.neighbors(node):
            partition_neighbors[self.partition[neighbor]] += 1
        
        # Current proxy requirement
        current_proxies = len([p for p in partition_neighbors.keys() 
                              if p != current_partition])
        
        # Proxy requirement after migration
        new_proxies = len([p for p in partition_neighbors.keys() 
                          if p != target_partition])
        
        proxy_gain = current_proxies - new_proxies
        
        # Check balance constraint
        partition_sizes = self.get_partition_sizes()
        avg_size = len(self.graph.nodes()) / self.num_partitions
        
        new_sizes = partition_sizes.copy()
        new_sizes[current_partition] -= 1
        new_sizes[target_partition] += 1
        
        # Penalize if balance violated
        balance_penalty = 0
        for size in new_sizes.values():
            if abs(size - avg_size) / avg_size > self.balance_tolerance:
                balance_penalty = 10  # Heavy penalty
        
        return proxy_gain - balance_penalty
    
    def get_partition_sizes(self):
        """Get current partition sizes"""
        sizes = defaultdict(int)
        for node, partition in self.partition.items():
            sizes[partition] += 1
        return sizes
    
    def refine(self, max_iterations=10):
        """Perform proxy-aware refinement"""
        print("Starting proxy-aware refinement...")
        
        for iteration in range(max_iterations):
            print(f"\nIteration {iteration + 1}/{max_iterations}")
            
            # Identify bridge vertices (high cross-partition connectivity)
            bridge_vertices = []
            for node in self.graph.nodes():
                partitions_connected = self.compute_partitions_connected(node)
                if partitions_connected > 0:
                    bridge_vertices.append((node, partitions_connected))
            
            # Sort by connectivity (most connected first)
            bridge_vertices.sort(key=lambda x: -x[1])
            
            moves_made = 0
            
            # Try to migrate each bridge vertex
            for node, _ in bridge_vertices[:len(bridge_vertices)//10]:  # Top 10%
                current_partition = self.partition[node]
                best_partition = current_partition
                best_gain = 0
                
                # Evaluate all target partitions
                for target_partition in range(self.num_partitions):
                    gain = self.compute_migration_gain(node, target_partition)
                    if gain > best_gain:
                        best_gain = gain
                        best_partition = target_partition
                
                # Make the move if beneficial
                if best_partition != current_partition and best_gain > 0:
                    self.partition[node] = best_partition
                    moves_made += 1
            
            print(f"Moves made: {moves_made}")
            
            if moves_made == 0:
                print("Converged (no beneficial moves)")
                break
        
        return self.partition

# Load graph and partition
with open('../data/graph.pkl', 'rb') as f:
    graph = pickle.load(f)['graph']

with open('../data/metis_partition.pkl', 'rb') as f:
    partition_data = pickle.load(f)
    partition_assignment = partition_data['partition_assignment']

# Refine
refiner = ProxyAwareRefinement(graph, partition_assignment)
refined_partition = refiner.refine(max_iterations=10)

# Analyze improvement
from partition_graph import GraphPartitioner
partitioner = GraphPartitioner(graph, num_partitions=3)
print("\n=== AFTER REFINEMENT ===")
refined_stats = partitioner.analyze_partition(refined_partition)

# Save refined partition
with open('../data/refined_partition.pkl', 'wb') as f:
    pickle.dump({
        'partition_assignment': refined_partition,
        'stats': refined_stats
    }, f)

print("\nRefined partition saved!")