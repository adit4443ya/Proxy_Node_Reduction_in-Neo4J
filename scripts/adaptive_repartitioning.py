import time
import pickle
from partition_graph import GraphPartitioner
from proxy_refinement import ProxyAwareRefinement
from load_to_neo4j import Neo4jShardLoader
from benchmark_queries import QueryBenchmark

import random

def load_workload_edge_weights():
    # Example: randomly assign weights to 10% of edges as hotspots
    edge_weights = {}
    
    with open('../data/graph.pkl', 'rb') as f:
        data = pickle.load(f)
        graph = data['graph']

    all_edges = list(graph.edges())
    hotspot_edges = random.sample(all_edges, k=int(0.1 * len(all_edges)))
    
    for u, v in all_edges:
        if (u, v) in hotspot_edges or (v, u) in hotspot_edges:
            edge_weights[(u, v)] = 10  # Hot edge, weight 10
        else:
            edge_weights[(u, v)] = 1   # Normal edge, weight 1
    
    return edge_weights


def compute_proxy_count(partition, graph):
    proxy_sets = {i: set() for i in range(3)}
    for u, v in graph.edges():
        part_u = partition[u]
        part_v = partition[v]
        if part_u != part_v:
            proxy_sets[part_u].add(v)
            proxy_sets[part_v].add(u)
    total_proxies = sum(len(s) for s in proxy_sets.values())
    return total_proxies

def adaptive_partition_loop(graph, initial_partition, interval_sec=600, max_cycles=10, threshold=0.1):
    partition = initial_partition
    old_proxy_count = None

    for cycle in range(max_cycles):
        print(f"\nAdaptive repartition cycle {cycle+1}/{max_cycles}")
        edge_weights = load_workload_edge_weights()
        partitioner = GraphPartitioner(graph, num_partitions=3, edge_weights=edge_weights)
        partition_assignment, _ = partitioner.partition_with_metis()

        refiner = ProxyAwareRefinement(graph, partition_assignment, edge_weights=edge_weights)
        refined_partition = refiner.refine(max_iters=5)

        proxy_count = compute_proxy_count(refined_partition, graph)
        print(f"Proxy count: {proxy_count}")
        if old_proxy_count is None or abs(proxy_count - old_proxy_count)/old_proxy_count > threshold:
            print("Significant proxy count change; reloading shards")
            loader = Neo4jShardLoader(refined_partition, graph)
            for pid in range(3):
                db_name = loader.db_names[pid]
                loader.clear_database(db_name)
                loader.load_partition_to_database(pid)
            
            benchmark = QueryBenchmark()
            benchmark.analyze_proxy_impact()
            benchmark.benchmark_neighbor_queries(num_queries=100)

            old_proxy_count = proxy_count
            partition = refined_partition
        else:
            print("No significant improvement, stopping adaptive repartition")
            break

        print(f"Sleeping for {interval_sec} seconds...")
        time.sleep(interval_sec)

if __name__ == "__main__":
    with open('../data/graph.pkl', 'rb') as f:
        data = pickle.load(f)
        graph = data['graph']
    with open('../data/metis_partition_weighted.pkl', 'rb') as f:
        partition_data = pickle.load(f)
        initial_partition = partition_data['partition_assignment']

    adaptive_partition_loop(graph, initial_partition)
