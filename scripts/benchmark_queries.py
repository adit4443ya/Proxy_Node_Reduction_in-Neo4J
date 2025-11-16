from neo4j import GraphDatabase
import time
import pickle
import random
import numpy as np
from collections import defaultdict

class QueryBenchmark:
    def __init__(self):
        self.uri = "bolt://localhost:7687"
        self.auth = ("neo4j", "password123")
    
    def run_query(self, query, database="mycomposite"):
        """Execute query and measure time"""
        driver = GraphDatabase.driver(self.uri, auth=self.auth)
        
        start = time.time()
        with driver.session(database=database) as session:
            result = session.run(query)
            records = list(result)
        elapsed = (time.time() - start) * 1000  # milliseconds
        
        driver.close()
        return elapsed, len(records)
    
    def benchmark_multi_hop_queries(self, num_queries=250, max_hops=3, query_mix=None, partition=None):
        """
        Comprehensive multi-hop query benchmark
        
        Args:
            num_queries: Total number of queries to execute
            max_hops: Maximum hop depth (1, 2, or 3)
            query_mix: Dict with percentages for each hop type
            partition: Current partition assignment (for cross-shard analysis)
        
        Returns:
            Dict with detailed timing and cross-shard statistics
        """
        print(f"\n{'='*60}")
        print(f"MULTI-HOP QUERY BENCHMARK")
        print(f"{'='*60}")
        print(f"Total queries: {num_queries}")
        print(f"Max hops: {max_hops}")
        
        if query_mix is None:
            query_mix = {'1-hop': 0.5, '2-hop': 0.3, '3-hop': 0.2}
        
        # Load graph for node sampling
        with open('../data/graph.pkl', 'rb') as f:
            graph = pickle.load(f)['graph']
        
        # Determine query counts per type
        query_counts = {
            '1-hop': int(num_queries * query_mix.get('1-hop', 0.5)),
            '2-hop': int(num_queries * query_mix.get('2-hop', 0.3)),
            '3-hop': int(num_queries * query_mix.get('3-hop', 0.2))
        }
        
        # Adjust for rounding
        query_counts['1-hop'] += num_queries - sum(query_counts.values())
        
        print(f"Query distribution: {query_counts}")
        
        # Results storage
        results_by_type = defaultdict(list)
        all_latencies = []
        cross_shard_hops = 0
        same_shard_hops = 0
        
        # Sample nodes (prefer high-degree for interesting traversals)
        degrees = dict(graph.degree())
        high_degree_nodes = sorted(graph.nodes(), key=lambda n: degrees.get(n, 0), reverse=True)[:1000]
        
        # Execute 1-hop queries
        if query_counts['1-hop'] > 0:
            print(f"\nExecuting {query_counts['1-hop']} 1-hop queries...")
            for i in range(query_counts['1-hop']):
                node_id = random.choice(high_degree_nodes)
                
                query = f"""
                    CALL {{
                        USE mycomposite.shard1
                        MATCH (p:Person {{id: {node_id}}})-[:KNOWS]->(friend)
                        RETURN friend.id as id, 'shard1' as shard
                        UNION
                        USE mycomposite.shard2
                        MATCH (p:Person {{id: {node_id}}})-[:KNOWS]->(friend)
                        RETURN friend.id as id, 'shard2' as shard
                        UNION
                        USE mycomposite.shard3
                        MATCH (p:Person {{id: {node_id}}})-[:KNOWS]->(friend)
                        RETURN friend.id as id, 'shard3' as shard
                    }}
                    RETURN id, shard
                """
                
                latency, count = self.run_query(query)
                results_by_type['1-hop'].append(latency)
                all_latencies.append(latency)
                
                # Analyze cross-shard hops
                if partition and node_id in graph.nodes():
                    for neighbor in graph.neighbors(node_id):
                        if partition.get(node_id) != partition.get(neighbor):
                            cross_shard_hops += 1
                        else:
                            same_shard_hops += 1
                
                if (i + 1) % 50 == 0:
                    print(f"  Completed {i+1}/{query_counts['1-hop']} 1-hop queries...")
        
        # Execute 2-hop queries
        if query_counts['2-hop'] > 0 and max_hops >= 2:
            print(f"\nExecuting {query_counts['2-hop']} 2-hop queries...")
            for i in range(query_counts['2-hop']):
                node_id = random.choice(high_degree_nodes)
                
                query = f"""
                    CALL {{
                        USE mycomposite.shard1
                        MATCH (p:Person {{id: {node_id}}})-[:KNOWS*1..2]->(fof)
                        RETURN fof.id as id, 'shard1' as shard
                        UNION
                        USE mycomposite.shard2
                        MATCH (p:Person {{id: {node_id}}})-[:KNOWS*1..2]->(fof)
                        RETURN fof.id as id, 'shard2' as shard
                        UNION
                        USE mycomposite.shard3
                        MATCH (p:Person {{id: {node_id}}})-[:KNOWS*1..2]->(fof)
                        RETURN fof.id as id, 'shard3' as shard
                    }}
                    RETURN DISTINCT id, shard
                """
                
                latency, count = self.run_query(query)
                results_by_type['2-hop'].append(latency)
                all_latencies.append(latency)
                
                # Estimate cross-shard hops (approximate)
                if partition and node_id in graph.nodes():
                    neighbors = list(graph.neighbors(node_id))
                    if neighbors:
                        for _ in range(min(5, len(neighbors))):
                            n1 = random.choice(neighbors)
                            if partition.get(node_id) != partition.get(n1):
                                cross_shard_hops += 1
                            else:
                                same_shard_hops += 1
                            
                            n2_neighbors = list(graph.neighbors(n1))
                            if n2_neighbors:
                                n2 = random.choice(n2_neighbors)
                                if partition.get(n1) != partition.get(n2):
                                    cross_shard_hops += 1
                                else:
                                    same_shard_hops += 1
                
                if (i + 1) % 50 == 0:
                    print(f"  Completed {i+1}/{query_counts['2-hop']} 2-hop queries...")
        
        # Execute 3-hop queries
        if query_counts['3-hop'] > 0 and max_hops >= 3:
            print(f"\nExecuting {query_counts['3-hop']} 3-hop queries...")
            for i in range(query_counts['3-hop']):
                node_id = random.choice(high_degree_nodes)
                
                query = f"""
                    CALL {{
                        USE mycomposite.shard1
                        MATCH (p:Person {{id: {node_id}}})-[:KNOWS*1..3]->(remote)
                        RETURN remote.id as id
                        UNION
                        USE mycomposite.shard2
                        MATCH (p:Person {{id: {node_id}}})-[:KNOWS*1..3]->(remote)
                        RETURN remote.id as id
                        UNION
                        USE mycomposite.shard3
                        MATCH (p:Person {{id: {node_id}}})-[:KNOWS*1..3]->(remote)
                        RETURN remote.id as id
                    }}
                    RETURN DISTINCT id LIMIT 100
                """
                
                latency, count = self.run_query(query)
                results_by_type['3-hop'].append(latency)
                all_latencies.append(latency)
                
                # Estimate cross-shard hops
                if partition and node_id in graph.nodes():
                    neighbors = list(graph.neighbors(node_id))
                    if neighbors:
                        for _ in range(min(3, len(neighbors))):
                            current = node_id
                            
                            for hop in range(3):
                                next_neighbors = list(graph.neighbors(current))
                                if not next_neighbors:
                                    break
                                
                                next_node = random.choice(next_neighbors)
                                if partition.get(current) != partition.get(next_node):
                                    cross_shard_hops += 1
                                else:
                                    same_shard_hops += 1
                                
                                current = next_node
                
                if (i + 1) % 50 == 0:
                    print(f"  Completed {i+1}/{query_counts['3-hop']} 3-hop queries...")
        
        # Compile results
        print(f"\n{'='*60}")
        print("BENCHMARK RESULTS")
        print(f"{'='*60}")
        
        for hop_type in ['1-hop', '2-hop', '3-hop']:
            if results_by_type[hop_type]:
                latencies = results_by_type[hop_type]
                print(f"\n{hop_type} queries ({len(latencies)} total):")
                print(f"  Avg latency:    {np.mean(latencies):.2f}ms")
                print(f"  Median latency: {np.median(latencies):.2f}ms")
                print(f"  P95 latency:    {np.percentile(latencies, 95):.2f}ms")
                print(f"  P99 latency:    {np.percentile(latencies, 99):.2f}ms")
        
        print(f"\nOverall Statistics:")
        print(f"  Total queries:       {len(all_latencies)}")
        print(f"  Avg latency:         {np.mean(all_latencies):.2f}ms")
        print(f"  Median latency:      {np.median(all_latencies):.2f}ms")
        print(f"  P95 latency:         {np.percentile(all_latencies, 95):.2f}ms")
        print(f"  P99 latency:         {np.percentile(all_latencies, 99):.2f}ms")
        
        print(f"\nCross-Shard Analysis:")
        total_hops = cross_shard_hops + same_shard_hops
        print(f"  Cross-shard hops:    {cross_shard_hops}")
        print(f"  Same-shard hops:     {same_shard_hops}")
        print(f"  Cross-shard ratio:   {cross_shard_hops/max(total_hops, 1)*100:.1f}%")
        print(f"{'='*60}\n")
        
        return {
            'results_by_type': dict(results_by_type),
            'all_latencies': all_latencies,
            'cross_shard_hops': cross_shard_hops,
            'same_shard_hops': same_shard_hops,
            'query_counts': query_counts
        }
    
    def benchmark_neighbor_queries(self, num_queries=100):
        """Legacy 1-hop benchmark for compatibility"""
        result = self.benchmark_multi_hop_queries(
            num_queries=num_queries,
            max_hops=1,
            query_mix={'1-hop': 1.0, '2-hop': 0.0, '3-hop': 0.0}
        )
        return result['all_latencies'], []
    
    def analyze_proxy_impact(self):
        """Analyze proxy node distribution"""
        print("\n=== Analyzing Proxy Distribution ===")
        
        driver = GraphDatabase.driver(self.uri, auth=self.auth)
        
        for shard_id in [1, 2, 3]:
            with driver.session(database=f"shard{shard_id}") as session:
                result = session.run("MATCH (n:Person) RETURN count(n) as count")
                person_count = result.single()['count']
                
                result = session.run("MATCH (n:PersonProxy) RETURN count(n) as count")
                proxy_count = result.single()['count']
                
                proxy_pct = 100 * proxy_count / (person_count + proxy_count) if person_count + proxy_count > 0 else 0
                
                print(f"  shard{shard_id}:")
                print(f"    Actual persons: {person_count}")
                print(f"    Proxy nodes: {proxy_count}")
                print(f"    Proxy percentage: {proxy_pct:.2f}%")
        
        driver.close()


if __name__ == "__main__":
    # Run comprehensive multi-hop benchmark
    benchmark = QueryBenchmark()
    
    # Load partition for cross-shard analysis
    try:
        with open('../data/refined_partition.pkl', 'rb') as f:
            partition = pickle.load(f)['partition_assignment']
    except:
        partition = None
    
    # Test with different configurations
    print("\n" + "="*70)
    print("TESTING MULTI-HOP QUERIES")
    print("="*70)
    
    results = benchmark.benchmark_multi_hop_queries(
        num_queries=1000,
        max_hops=3,
        query_mix={'1-hop': 0.5, '2-hop': 0.3, '3-hop': 0.2},
        partition=partition
    )
    
    # Analyze proxy impact
    benchmark.analyze_proxy_impact()
    
    # Save results
    with open('../results/benchmark_multi_hop.pkl', 'wb') as f:
        pickle.dump(results, f)
    
    print("\n✓ Multi-hop benchmark complete!")
    print(f"✓ Results saved to ../results/benchmark_multi_hop.pkl")