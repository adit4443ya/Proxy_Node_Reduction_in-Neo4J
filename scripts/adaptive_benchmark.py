"""
Adaptive Query Benchmark with Dynamic Proxy Management

This benchmark runs queries while monitoring access patterns and dynamically
optimizing proxy node placement to reduce latency.
"""

import time
import pickle
import random
from typing import List, Dict, Tuple
from neo4j import GraphDatabase
import networkx as nx
from statistics import mean, median

from query_monitor import QueryMonitor
from proxy_scorer import ProxyScorer
from dynamic_proxy_manager import DynamicProxyManager
import config


class AdaptiveBenchmark:
    """
    Runs queries with adaptive proxy management.

    This benchmark:
    1. Executes queries across composite database
    2. Monitors access patterns in real-time
    3. Periodically evaluates and optimizes proxy placement
    4. Measures performance improvements over time
    """

    def __init__(self, graph_file: str = "../data/graph.pkl",
                 partition_file: str = "../data/refined_partition.pkl"):
        """
        Initialize adaptive benchmark.

        Args:
            graph_file: Path to original graph
            partition_file: Path to partition assignment
        """
        # Load data
        self.load_graph(graph_file)
        self.load_partition(partition_file)

        # Initialize components
        self.monitor = QueryMonitor()
        self.monitor.load_partition_data(partition_file)

        self.scorer = ProxyScorer(self.monitor, self.graph)
        self.scorer.load_graph(graph_file)

        self.manager = DynamicProxyManager(self.monitor, self.scorer)
        self.manager.connect_to_neo4j()

        # Query tracking
        self.query_results = []
        self.latency_windows = []  # Track latency in windows for trend analysis

        # Sample nodes for queries
        self.sample_nodes = self.select_sample_nodes(config.ADAPTIVE_BENCHMARK_QUERIES)

        if config.VERBOSE:
            print("✓ AdaptiveBenchmark initialized")
            print(f"  - {len(self.graph.nodes())} nodes in graph")
            print(f"  - {config.ADAPTIVE_BENCHMARK_QUERIES} queries planned")
            print(f"  - Optimization every {config.EVALUATION_FREQUENCY} queries")

    def load_graph(self, graph_file: str):
        """Load original graph"""
        with open(graph_file, 'rb') as f:
            data = pickle.load(f)
            self.graph = data['graph']

    def load_partition(self, partition_file: str):
        """Load partition assignment"""
        with open(partition_file, 'rb') as f:
            data = pickle.load(f)
            self.partition_assignment = data['partition_assignment']

    def select_sample_nodes(self, num_queries: int) -> List[int]:
        """
        Select sample nodes for queries.

        Uses weighted sampling to favor high-degree nodes (more interesting queries).

        Args:
            num_queries: Number of queries to generate

        Returns:
            List of node IDs to query
        """
        nodes = list(self.graph.nodes())
        degrees = [self.graph.degree(n) for n in nodes]

        # Weighted sampling by degree
        total_degree = sum(degrees)
        probabilities = [d / total_degree for d in degrees]

        sample = random.choices(nodes, weights=probabilities, k=num_queries)
        return sample

    def run_neighbor_query(self, node_id: int) -> Tuple[float, int]:
        """
        Execute a 1-hop neighbor query across all shards.

        Args:
            node_id: Node to query neighbors for

        Returns:
            Tuple of (latency_ms, result_count)
        """
        query = """
        CALL {
            USE mycomposite.shard1
            MATCH (p:Person {id: $node_id})-[:KNOWS]->(friend)
            RETURN friend
            UNION
            USE mycomposite.shard2
            MATCH (p:Person {id: $node_id})-[:KNOWS]->(friend)
            RETURN friend
            UNION
            USE mycomposite.shard3
            MATCH (p:Person {id: $node_id})-[:KNOWS]->(friend)
            RETURN friend
        }
        RETURN count(friend) as total
        """

        start_time = time.time()

        with self.manager.driver.session(database=config.COMPOSITE_DATABASE) as session:
            result = session.run(query, node_id=node_id)
            record = result.single()
            count = record['total'] if record else 0

        elapsed_ms = (time.time() - start_time) * 1000

        return elapsed_ms, count

    def run_query_with_monitoring(self, node_id: int, query_index: int) -> Dict:
        """
        Run a query and record monitoring data.

        Args:
            node_id: Node to query
            query_index: Index of this query in the benchmark

        Returns:
            Dictionary with query results and metadata
        """
        # Determine which shard this node belongs to
        home_shard = self.partition_assignment.get(node_id, 0)

        # Execute query
        latency_ms, result_count = self.run_neighbor_query(node_id)

        # Record access in monitor
        # Note: In a real scenario, we'd track which shards were actually accessed
        # For simplicity, we assume cross-shard access for now
        for shard_id in range(len(config.SHARD_DATABASES)):
            if shard_id != home_shard:
                self.monitor.record_access(
                    node_id=node_id,
                    accessing_shard=shard_id,
                    latency_ms=latency_ms / 2,  # Distribute latency
                    is_proxy=True
                )

        # Track result
        result = {
            'query_index': query_index,
            'node_id': node_id,
            'home_shard': home_shard,
            'latency_ms': latency_ms,
            'result_count': result_count,
            'timestamp': time.time()
        }

        return result

    def should_optimize(self, query_index: int) -> bool:
        """
        Determine if optimization should run.

        Args:
            query_index: Current query index

        Returns:
            True if should optimize
        """
        # Optimize at regular intervals
        if (query_index > 0 and
            query_index % config.EVALUATION_FREQUENCY == 0):
            return True
        return False

    def run_benchmark(self):
        """
        Run the adaptive benchmark.

        Executes queries while periodically optimizing proxy placement.
        """
        print("\n" + "=" * 70)
        print("STARTING ADAPTIVE BENCHMARK")
        print("=" * 70)
        print(f"Total Queries: {len(self.sample_nodes)}")
        print(f"Strategy: {config.MANAGEMENT_STRATEGY}")
        print(f"Optimization Frequency: every {config.EVALUATION_FREQUENCY} queries")
        print("=" * 70 + "\n")

        start_time = time.time()
        window_latencies = []

        for i, node_id in enumerate(self.sample_nodes):
            # Run query
            result = self.run_query_with_monitoring(node_id, i)
            self.query_results.append(result)
            window_latencies.append(result['latency_ms'])

            # Check if optimization should run
            if self.should_optimize(i):
                # Calculate window statistics
                window_avg = mean(window_latencies)
                self.latency_windows.append({
                    'query_index': i,
                    'avg_latency': window_avg,
                    'queries_in_window': len(window_latencies)
                })

                print(f"\n[Query {i}/{len(self.sample_nodes)}] "
                      f"Window Avg Latency: {window_avg:.2f}ms")

                # Run optimization
                actions = self.manager.evaluate_and_optimize()

                # Print scoring summary
                if config.VERBOSE:
                    self.scorer.print_top_scores(k=3)

                # Reset window
                window_latencies = []

            # Progress reporting
            if i > 0 and i % config.REPORT_FREQUENCY == 0:
                elapsed = time.time() - start_time
                qps = i / elapsed
                avg_latency = mean([r['latency_ms'] for r in self.query_results])

                print(f"[Progress] {i}/{len(self.sample_nodes)} queries "
                      f"| {qps:.1f} q/s | avg latency: {avg_latency:.2f}ms")

        # Final window
        if window_latencies:
            self.latency_windows.append({
                'query_index': len(self.sample_nodes),
                'avg_latency': mean(window_latencies),
                'queries_in_window': len(window_latencies)
            })

        total_time = time.time() - start_time

        print("\n" + "=" * 70)
        print("BENCHMARK COMPLETED")
        print("=" * 70)
        print(f"Total Time: {total_time:.2f} seconds")
        print(f"Queries Executed: {len(self.query_results)}")
        print(f"Avg QPS: {len(self.query_results) / total_time:.2f}")
        print("=" * 70 + "\n")

    def analyze_results(self):
        """Analyze and print benchmark results"""
        if not self.query_results:
            print("✗ No results to analyze")
            return

        latencies = [r['latency_ms'] for r in self.query_results]

        print("\n" + "=" * 70)
        print("BENCHMARK ANALYSIS")
        print("=" * 70)

        # Overall latency statistics
        print("\nLatency Statistics:")
        print(f"  Mean:   {mean(latencies):.2f} ms")
        print(f"  Median: {median(latencies):.2f} ms")
        print(f"  Min:    {min(latencies):.2f} ms")
        print(f"  Max:    {max(latencies):.2f} ms")

        # Percentiles
        sorted_latencies = sorted(latencies)
        p95_index = int(len(sorted_latencies) * 0.95)
        p99_index = int(len(sorted_latencies) * 0.99)
        print(f"  P95:    {sorted_latencies[p95_index]:.2f} ms")
        print(f"  P99:    {sorted_latencies[p99_index]:.2f} ms")

        # Window trend analysis
        if len(self.latency_windows) > 1:
            print("\nLatency Trend (by window):")
            first_window = self.latency_windows[0]['avg_latency']
            last_window = self.latency_windows[-1]['avg_latency']
            improvement = ((first_window - last_window) / first_window) * 100

            for window in self.latency_windows:
                print(f"  Query {window['query_index']}: "
                      f"{window['avg_latency']:.2f}ms")

            print(f"\nOverall Improvement: {improvement:+.1f}%")
            print(f"  First window: {first_window:.2f}ms")
            print(f"  Last window:  {last_window:.2f}ms")

        # Query monitoring summary
        print("\n")
        self.monitor.print_summary()

        # Optimization statistics
        print("\n")
        self.manager.print_statistics()

        print("=" * 70)

    def save_results(self, filepath: str = "../results/adaptive_benchmark_results.pkl"):
        """Save benchmark results to file"""
        data = {
            'query_results': self.query_results,
            'latency_windows': self.latency_windows,
            'monitor_stats': self.monitor.get_statistics(),
            'manager_stats': self.manager.get_statistics(),
            'config': {
                'strategy': config.MANAGEMENT_STRATEGY,
                'evaluation_frequency': config.EVALUATION_FREQUENCY,
                'total_queries': config.ADAPTIVE_BENCHMARK_QUERIES,
            }
        }

        with open(filepath, 'wb') as f:
            pickle.dump(data, f)

        if config.VERBOSE:
            print(f"\n✓ Results saved to {filepath}")

        # Also save monitoring and optimization data
        if config.SAVE_MONITORING_DATA:
            self.monitor.save_to_file()

        if config.SAVE_OPTIMIZATION_HISTORY:
            self.manager.save_history()

    def cleanup(self):
        """Clean up resources"""
        self.manager.close()


def main():
    """Main entry point for adaptive benchmark"""
    print("=" * 70)
    print("ADAPTIVE PROXY MANAGEMENT BENCHMARK")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  Management Strategy:     {config.MANAGEMENT_STRATEGY}")
    print(f"  Total Queries:           {config.ADAPTIVE_BENCHMARK_QUERIES}")
    print(f"  Evaluation Frequency:    every {config.EVALUATION_FREQUENCY} queries")
    print(f"  Promotion Enabled:       {config.ENABLE_PROMOTION}")
    print(f"  Replication Enabled:     {config.ENABLE_REPLICATION}")
    print(f"  Max Replication:         {config.MAX_REPLICATION_PERCENTAGE * 100}%")
    print()

    # Run benchmark
    benchmark = AdaptiveBenchmark()

    try:
        benchmark.run_benchmark()
        benchmark.analyze_results()
        benchmark.save_results()
    finally:
        benchmark.cleanup()

    print("\n✓ Adaptive benchmark completed successfully!\n")


if __name__ == "__main__":
    main()
