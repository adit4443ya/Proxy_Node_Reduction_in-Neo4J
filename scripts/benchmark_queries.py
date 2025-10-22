from neo4j import GraphDatabase
import time
import pickle
import random
import numpy as np

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
    
    def benchmark_neighbor_queries(self, num_queries=100):
        """Benchmark 1-hop neighbor queries"""
        print("\n=== Benchmarking Neighbor Queries ===")
        
        # Load graph to sample nodes
        with open('../data/graph.pkl', 'rb') as f:
            graph = pickle.load(f)['graph']
        
        sample_nodes = random.sample(list(graph.nodes()), num_queries)
        latencies = []
        result_counts = []
        
        for i, node_id in enumerate(sample_nodes):
            query = f"""
                CALL {{
                    USE mycomposite.shard1
                    MATCH (p:Person {{id: {node_id}}})-[:KNOWS]->(friend)
                    RETURN friend
                    UNION
                    USE mycomposite.shard2
                    MATCH (p:Person {{id: {node_id}}})-[:KNOWS]->(friend)
                    RETURN friend
                    UNION
                    USE mycomposite.shard3
                    MATCH (p:Person {{id: {node_id}}})-[:KNOWS]->(friend)
                    RETURN friend
                }}
                RETURN count(friend) as total
            """
            
            latency, count = self.run_query(query)
            latencies.append(latency)
            result_counts.append(count)
            
            if (i+1) % 20 == 0:
                print(f"  Completed {i+1}/{num_queries} queries...")
        
        print(f"\nResults:")
        print(f"  Avg latency: {np.mean(latencies):.2f}ms")
        print(f"  Median latency: {np.median(latencies):.2f}ms")
        print(f"  P95 latency: {np.percentile(latencies, 95):.2f}ms")
        print(f"  P99 latency: {np.percentile(latencies, 99):.2f}ms")
        print(f"  Avg results per query: {np.mean(result_counts):.1f}")
        
        return latencies, result_counts
    
    def analyze_proxy_impact(self):
        """Analyze proxy node distribution"""
        print("\n=== Analyzing Proxy Distribution ===")
        
        driver = GraphDatabase.driver(self.uri, auth=self.auth)
        
        for shard_id in [1, 2, 3]:
            with driver.session(database=f"shard{shard_id}") as session:
                # Count actual nodes
                result = session.run("MATCH (n:Person) RETURN count(n) as count")
                person_count = result.single()['count']
                
                # Count proxy nodes
                result = session.run("MATCH (n:PersonProxy) RETURN count(n) as count")
                proxy_count = result.single()['count']
                
                proxy_pct = 100 * proxy_count / (person_count + proxy_count) if person_count + proxy_count > 0 else 0
                
                print(f"  shard{shard_id}:")
                print(f"    Actual persons: {person_count}")
                print(f"    Proxy nodes: {proxy_count}")
                print(f"    Proxy percentage: {proxy_pct:.2f}%")
        
        driver.close()

# Run benchmarks
benchmark = QueryBenchmark()
benchmark.analyze_proxy_impact()
latencies, counts = benchmark.benchmark_neighbor_queries(num_queries=100)

# Save results
with open('../results/benchmark_results.pkl', 'wb') as f:
    pickle.dump({
        'neighbor_latencies': latencies,
        'result_counts': counts
    }, f)

print("\n✓ Benchmark complete! Results saved to results/benchmark_results.pkl")