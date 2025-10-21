from neo4j import GraphDatabase
import time
import pickle
import random

class QueryBenchmark:
    def __init__(self):
        self.composite_uri = "bolt://localhost:7687"
        self.auth = ("neo4j", "password123")
    
    def run_query(self, query, database="mycomposite"):
        """Execute query and measure time"""
        driver = GraphDatabase.driver(self.composite_uri, auth=self.auth)
        
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
        
        for node_id in sample_nodes:
            query = f"""
                MATCH (p:Person {{id: {node_id}}})-[:KNOWS]->(friend)
                RETURN friend.id, friend.name
            """
            latency, count = self.run_query(query)
            latencies.append(latency)
        
        print(f"Avg latency: {sum(latencies)/len(latencies):.2f}ms")
        print(f"P95 latency: {sorted(latencies)[int(0.95*len(latencies))]:.2f}ms")
        print(f"P99 latency: {sorted(latencies)[int(0.99*len(latencies))]:.2f}ms")
        
        return latencies
    
    def count_cross_shard_queries(self):
        """Count how many queries need cross-shard access"""
        # This requires analyzing query plans
        # Simplified version for now
        pass

# Run benchmark
benchmark = QueryBenchmark()
latencies = benchmark.benchmark_neighbor_queries(num_queries=100)

# Save results
with open('../results/benchmark_results.pkl', 'wb') as f:
    pickle.dump({'neighbor_latencies': latencies}, f)