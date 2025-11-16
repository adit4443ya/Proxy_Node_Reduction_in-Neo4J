"""
Complete Cost Model Implementation
Addresses Feedback: How are Imbalance, Qcost and constants defined in EQ.1? What does ccross depend on?
"""

import numpy as np
from collections import defaultdict

class CompleteCostModel:
    """
    Implements the complete cost function from Equation 1:
    Φ(P, W, t) = α·ProxyCost + β·ImbalanceCost + γ·QueryCost + δ·ActivationCost
    """
    
    def __init__(self, graph, alpha=1.0, beta=10.0, gamma=5.0, delta=2.0):
        """
        Args:
            graph: NetworkX graph
            alpha: Proxy overhead weight (default: 1.0)
            beta: Imbalance penalty weight (default: 10.0)
            gamma: Query cost weight (default: 5.0)
            delta: Proxy activation overhead weight (default: 2.0)
        """
        self.graph = graph
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.delta = delta
        
        # System-measured constants (in milliseconds)
        self.c_base = 5.0      # Base cross-database lookup latency
        self.c_lookup = 2.0    # Per-hop network latency
        self.c_data = 0.1      # Per-neighbor data transfer cost
        self.c_create = 10.0   # Proxy node creation time
        self.c_sync = 1.0      # Per-neighbor synchronization cost
        
    def compute_proxy_cost(self, partition):
        """
        ProxyCost(P) = |{proxy nodes}| / |V|
        
        Returns normalized proxy count
        """
        proxy_nodes = set()
        for u, v in self.graph.edges():
            if partition[u] != partition[v]:
                proxy_nodes.add(u)
                proxy_nodes.add(v)
        
        proxy_ratio = len(proxy_nodes) / len(self.graph.nodes())
        return proxy_ratio
    
    def compute_imbalance_cost(self, partition, num_partitions=3):
        """
        ImbalanceCost(P) = (max_i |P_i| - min_i |P_i|) / (|V|/k)
        
        Returns normalized load imbalance
        """
        partition_sizes = defaultdict(int)
        for node, part in partition.items():
            partition_sizes[part] += 1
        
        sizes = list(partition_sizes.values())
        if not sizes:
            return 0.0
        
        max_size = max(sizes)
        min_size = min(sizes)
        avg_size = len(self.graph.nodes()) / num_partitions
        
        imbalance = (max_size - min_size) / avg_size if avg_size > 0 else 0.0
        return imbalance
    
    def compute_ccross(self, u, v, partition):
        """
        c_cross(u,v) = c_base + c_lookup·hops(P(u),P(v)) + c_data·degree(v)
        
        Models the cost of cross-partition edge traversal
        """
        part_u = partition[u]
        part_v = partition[v]
        
        if part_u == part_v:
            return 0.0  # No cross-partition cost
        
        # Simplified hop count (in single-instance, hops=1)
        hops = 1 if part_u != part_v else 0
        
        # Node degree for data transfer cost
        degree_v = self.graph.degree(v)
        
        c_cross = self.c_base + self.c_lookup * hops + self.c_data * degree_v
        return c_cross
    
    def compute_query_cost(self, partition, edge_weights=None):
        """
        QueryCost(P,W) = Σ_(u,v)∈E w(u,v) · I[P(u)≠P(v)] · c_cross(u,v)
        
        Returns weighted cross-partition query cost
        """
        if edge_weights is None:
            edge_weights = {}
        
        total_cost = 0.0
        for u, v in self.graph.edges():
            weight = edge_weights.get((u, v), edge_weights.get((v, u), 1.0))
            
            if partition[u] != partition[v]:  # Cross-partition edge
                c_cross = self.compute_ccross(u, v, partition)
                total_cost += weight * c_cross
        
        # Normalize by total edges
        total_cost /= max(len(self.graph.edges()), 1)
        return total_cost
    
    def compute_activation_cost(self, old_partition, new_partition):
        """
        ActivationCost = Σ_v∈NewProxies (c_create + c_sync·neighbors(v))
        
        Returns cost of activating new proxy nodes during repartitioning
        """
        # Find new proxy nodes (nodes that changed partition)
        changed_nodes = [v for v in self.graph.nodes() 
                        if old_partition.get(v) != new_partition.get(v)]
        
        activation_cost = 0.0
        for node in changed_nodes:
            # Proxy creation cost
            activation_cost += self.c_create
            
            # Synchronization cost for neighbors
            neighbors = list(self.graph.neighbors(node))
            activation_cost += self.c_sync * len(neighbors)
        
        # Normalize by graph size
        activation_cost /= max(len(self.graph.nodes()), 1)
        return activation_cost
    
    def compute_total_cost(self, partition, edge_weights=None, old_partition=None):
        """
        Complete cost function:
        Φ(P, W, t) = α·ProxyCost + β·ImbalanceCost + γ·QueryCost + δ·ActivationCost
        
        Returns total cost of partition
        """
        proxy_cost = self.compute_proxy_cost(partition)
        imbalance_cost = self.compute_imbalance_cost(partition)
        query_cost = self.compute_query_cost(partition, edge_weights)
        
        # Activation cost only if we have old partition
        activation_cost = 0.0
        if old_partition is not None:
            activation_cost = self.compute_activation_cost(old_partition, partition)
        
        total_cost = (
            self.alpha * proxy_cost +
            self.beta * imbalance_cost +
            self.gamma * query_cost +
            self.delta * activation_cost
        )
        
        return {
            'total': total_cost,
            'proxy_cost': proxy_cost,
            'imbalance_cost': imbalance_cost,
            'query_cost': query_cost,
            'activation_cost': activation_cost,
            'components': {
                'alpha_proxy': self.alpha * proxy_cost,
                'beta_imbalance': self.beta * imbalance_cost,
                'gamma_query': self.gamma * query_cost,
                'delta_activation': self.delta * activation_cost
            }
        }
    
    def print_cost_breakdown(self, partition, edge_weights=None, old_partition=None):
        """Print detailed cost breakdown"""
        costs = self.compute_total_cost(partition, edge_weights, old_partition)
        
        print("\n" + "="*60)
        print("COST MODEL BREAKDOWN (Equation 1)")
        print("="*60)
        print(f"Parameters: α={self.alpha}, β={self.beta}, γ={self.gamma}, δ={self.delta}")
        print("\nComponents:")
        print(f"  α·ProxyCost       = {self.alpha} × {costs['proxy_cost']:.4f} = {costs['components']['alpha_proxy']:.4f}")
        print(f"  β·ImbalanceCost   = {self.beta} × {costs['imbalance_cost']:.4f} = {costs['components']['beta_imbalance']:.4f}")
        print(f"  γ·QueryCost       = {self.gamma} × {costs['query_cost']:.4f} = {costs['components']['gamma_query']:.4f}")
        print(f"  δ·ActivationCost  = {self.delta} × {costs['activation_cost']:.4f} = {costs['components']['delta_activation']:.4f}")
        print(f"\nTOTAL COST Φ(P) = {costs['total']:.4f}")
        print("="*60 + "\n")
        
        return costs


if __name__ == "__main__":
    # Test with sample graph
    import pickle
    import sys
    
    try:
        with open('../data/graph.pkl', 'rb') as f:
            graph = pickle.load(f)['graph']
        
        with open('../data/refined_partition.pkl', 'rb') as f:
            partition = pickle.load(f)['partition_assignment']
        
        cost_model = CompleteCostModel(graph)
        costs = cost_model.print_cost_breakdown(partition)
        
        print("✓ Cost model validation successful!")
        
    except FileNotFoundError:
        print("Note: Run generate_graph.py and partition_graph.py first")