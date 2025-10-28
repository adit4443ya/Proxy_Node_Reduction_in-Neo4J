import networkx as nx
import pickle
import pymetis
from collections import defaultdict

class GraphPartitioner:
    def __init__(self, graph, num_partitions=3, edge_weights=None):
        self.graph = graph
        self.num_partitions = num_partitions
        self.edge_weights = edge_weights or {}
    
    def graph_to_metis_format(self):
        node_mapping = {node: idx for idx, node in enumerate(self.graph.nodes())}
        reverse_mapping = {idx: node for node, idx in node_mapping.items()}

        adjacency = [[] for _ in range(len(node_mapping))]
        for u, v in self.graph.edges():
            u_idx = node_mapping[u]
            v_idx = node_mapping[v]
            adjacency[u_idx].append(v_idx)
            adjacency[v_idx].append(u_idx)

        # Flatten edge weights corresponding to adjacency edges
        eweights = []
        for u_idx in range(len(adjacency)):
            for v_idx in adjacency[u_idx]:
                u = reverse_mapping[u_idx]
                v = reverse_mapping[v_idx]
                w = self.edge_weights.get((u, v), self.edge_weights.get((v, u), 1))
                eweights.append(int(w))  # pymetis expects int weights

        return adjacency, eweights, node_mapping, reverse_mapping
    
    def partition_with_metis(self):
        adjacency, eweights, node_mapping, reverse_mapping = self.graph_to_metis_format()
        print(f"Running weighted METIS partitioning with k={self.num_partitions}...")
        n_cuts, membership = pymetis.part_graph(
            nparts=self.num_partitions,
            adjacency=adjacency,
            eweights=eweights
        )
        print(f"Weighted edge-cut: {n_cuts}")
        partition_assignment = {reverse_mapping[idx]: part for idx, part in enumerate(membership)}
        return partition_assignment, n_cuts
    
    def analyze_partition(self, partition_assignment):
        stats = {'partition_sizes': defaultdict(int), 'edge_cuts': 0, 'proxy_counts': defaultdict(set)}
        for node, part in partition_assignment.items():
            stats['partition_sizes'][part] += 1
        
        for u, v in self.graph.edges():
            part_u, part_v = partition_assignment[u], partition_assignment[v]
            if part_u != part_v:
                stats['edge_cuts'] += 1
                stats['proxy_counts'][part_v].add(u)
                stats['proxy_counts'][part_u].add(v)
        
        total_proxies = sum(len(proxies) for proxies in stats['proxy_counts'].values())
        print("\nPartition Stats")
        print(f"Sizes: {dict(stats['partition_sizes'])}")
        print(f"Edge Cuts: {stats['edge_cuts']}")
        print(f"Proxies: {total_proxies} ({100 * total_proxies/len(self.graph.nodes()):.2f}%)")
        return stats


# Main execution for testing / running
if __name__ == "__main__":
    with open('../data/graph.pkl', 'rb') as f:
        data = pickle.load(f)
        graph = data['graph']

    # TODO: load real workload edge weights here
    edge_weights = {}

    partitioner = GraphPartitioner(graph, num_partitions=3, edge_weights=edge_weights)
    partition_assignment, edge_cuts = partitioner.partition_with_metis()
    stats = partitioner.analyze_partition(partition_assignment)

    with open('../data/metis_partition_weighted.pkl', 'wb') as f:
        pickle.dump({
            'partition_assignment': partition_assignment,
            'edge_cuts': edge_cuts,
            'stats': stats
        }, f)
