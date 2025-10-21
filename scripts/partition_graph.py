import networkx as nx
import pickle
import pymetis
import numpy as np
from collections import defaultdict

class GraphPartitioner:
    def __init__(self, graph, num_partitions=3):
        self.graph = graph
        self.num_partitions = num_partitions
        
    def graph_to_metis_format(self):
        """Convert NetworkX graph to METIS adjacency format"""
        # METIS uses 0-based indexing
        node_mapping = {node: idx for idx, node in enumerate(self.graph.nodes())}
        reverse_mapping = {idx: node for node, idx in node_mapping.items()}
        
        # Build adjacency lists
        adjacency = [[] for _ in range(len(node_mapping))]
        for u, v in self.graph.edges():
            u_idx = node_mapping[u]
            v_idx = node_mapping[v]
            adjacency[u_idx].append(v_idx)
            adjacency[v_idx].append(u_idx)
        
        return adjacency, node_mapping, reverse_mapping
    
    def partition_with_metis(self):
        """Run METIS k-way partitioning"""
        adjacency, node_mapping, reverse_mapping = self.graph_to_metis_format()
        
        print(f"Running METIS k-way partitioning (k={self.num_partitions})...")
        
        # Run METIS
        n_cuts, membership = pymetis.part_graph(
            nparts=self.num_partitions,
            adjacency=adjacency
        )
        
        print(f"METIS edge-cut: {n_cuts}")
        
        # Map back to original node IDs
        partition_assignment = {}
        for idx, partition_id in enumerate(membership):
            original_node = reverse_mapping[idx]
            partition_assignment[original_node] = partition_id
        
        return partition_assignment, n_cuts
    
    def analyze_partition(self, partition_assignment):
        """Compute partition statistics"""
        stats = {
            'partition_sizes': defaultdict(int),
            'edge_cuts': 0,
            'proxy_counts': defaultdict(set)
        }
        
        # Compute partition sizes
        for node, partition in partition_assignment.items():
            stats['partition_sizes'][partition] += 1
        
        # Compute edge-cuts and proxy requirements
        for u, v in self.graph.edges():
            part_u = partition_assignment[u]
            part_v = partition_assignment[v]
            
            if part_u != part_v:
                stats['edge_cuts'] += 1
                # Node u needs proxy in partition of v
                stats['proxy_counts'][part_v].add(u)
                # Node v needs proxy in partition of u
                stats['proxy_counts'][part_u].add(v)
        
        # Convert proxy sets to counts
        total_proxies = sum(len(proxies) for proxies in stats['proxy_counts'].values())
        
        print("\n=== Partition Statistics ===")
        print(f"Partition sizes: {dict(stats['partition_sizes'])}")
        print(f"Edge cuts: {stats['edge_cuts']}")
        print(f"Total proxy nodes needed: {total_proxies}")
        print(f"Proxy percentage: {100 * total_proxies / len(self.graph.nodes()):.2f}%")
        
        return stats

# Load graph
with open('../data/graph.pkl', 'rb') as f:
    data = pickle.load(f)
    graph = data['graph']

# Partition
partitioner = GraphPartitioner(graph, num_partitions=3)
partition_assignment, edge_cuts = partitioner.partition_with_metis()
stats = partitioner.analyze_partition(partition_assignment)

# Save partition
with open('../data/metis_partition.pkl', 'wb') as f:
    pickle.dump({
        'partition_assignment': partition_assignment,
        'edge_cuts': edge_cuts,
        'stats': stats
    }, f)

print("\nPartition saved to data/metis_partition.pkl")