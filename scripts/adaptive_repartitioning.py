import time
import pickle

class AdaptiveRepartitioning:
    def __init__(self, graph, initial_partition, load_edge_weights_fn, partitioner, refiner, threshold=0.1):
        self.graph = graph
        self.partition = initial_partition
        self.load_edge_weights = load_edge_weights_fn
        self.partitioner = partitioner
        self.refiner = refiner
        self.threshold = threshold
        self.last_proxy_count = None
    
    def compute_proxy_count(self, partition):
        proxy_counts = defaultdict(set)
        for u, v in self.graph.edges():
            p_u = partition[u]
            p_v = partition[v]
            if p_u != p_v:
                proxy_counts[p_u].add(v)
                proxy_counts[p_v].add(u)
        return sum(len(s) for s in proxy_counts.values())
    
    def should_repartition(self, new_proxy_count):
        if self.last_proxy_count is None:
            return True
        change = abs(new_proxy_count - self.last_proxy_count) / self.last_proxy_count
        return change >= self.threshold
    
    def run(self, max_cycles=10, interval_sec=60):
        for cycle in range(max_cycles):
            print(f"Adaptive repartitioning cycle {cycle+1}")
            edge_weights = self.load_edge_weights()
            self.partitioner.edge_weights = edge_weights
            self.refiner.edge_weights = edge_weights
            
            partition, _ = self.partitioner.partition_with_metis()
            proxy_count = self.compute_proxy_count(partition)
            
            if self.should_repartition(proxy_count):
                print(f"Proxy count changed significantly: {proxy_count}, repartitioning...")
                refined_partition = self.refiner.refine(max_iters=5)
                self.partition = refined_partition
                self.last_proxy_count = proxy_count
                with open('../data/adaptive_partition.pkl', 'wb') as f:
                    pickle.dump({'partition_assignment': self.partition}, f)
            else:
                print("Proxy count stable, no repartitioning.")
            
            time.sleep(interval_sec)
