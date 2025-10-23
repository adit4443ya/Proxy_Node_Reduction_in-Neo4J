"""
Query Monitoring System for Dynamic Proxy Management

This module tracks query access patterns across Neo4j shards to identify
which proxy nodes should be promoted or replicated.
"""

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional
import pickle
from datetime import datetime
import config


@dataclass
class ProxyAccessStats:
    """Statistics for a single proxy node access pattern"""
    node_id: int
    home_shard: int  # Where the actual Person node lives

    # Access tracking
    total_accesses: int = 0
    access_by_shard: Dict[int, int] = field(default_factory=lambda: defaultdict(int))

    # Latency tracking
    total_latency: float = 0.0
    latency_history: deque = field(default_factory=lambda: deque(maxlen=100))

    # Timestamp tracking
    first_seen: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)

    # Cross-shard tracking
    cross_shard_accesses: int = 0
    local_accesses: int = 0

    @property
    def avg_latency(self) -> float:
        """Average latency for accessing this proxy"""
        if not self.latency_history:
            return 0.0
        return sum(self.latency_history) / len(self.latency_history)

    @property
    def access_frequency(self) -> float:
        """Queries per minute"""
        time_span = max(time.time() - self.first_seen, 1.0)  # Avoid division by zero
        return (self.total_accesses / time_span) * 60.0  # Convert to per minute

    @property
    def cross_shard_ratio(self) -> float:
        """Ratio of cross-shard to total accesses"""
        if self.total_accesses == 0:
            return 0.0
        return self.cross_shard_accesses / self.total_accesses

    @property
    def time_since_last_access(self) -> float:
        """Seconds since last access"""
        return time.time() - self.last_accessed


class QueryMonitor:
    """
    Monitors query patterns to identify optimization opportunities.

    This class tracks:
    - Which nodes are accessed from which shards
    - Access frequencies and patterns
    - Query latencies
    - Cross-shard vs local accesses
    """

    def __init__(self):
        self.proxy_stats: Dict[int, ProxyAccessStats] = {}
        self.query_count = 0
        self.start_time = time.time()

        # Track which nodes belong to which shards (loaded from partition data)
        self.node_to_shard: Dict[int, int] = {}

        # Query history for pattern detection
        self.query_history: deque = deque(maxlen=config.PATTERN_DETECTION_WINDOW)

        # Performance metrics
        self.total_latency = 0.0
        self.cross_shard_queries = 0
        self.local_queries = 0

        if config.VERBOSE:
            print("✓ QueryMonitor initialized")

    def load_partition_data(self, partition_file: str = "../data/refined_partition.pkl"):
        """Load partition assignment to know where each node lives"""
        try:
            with open(partition_file, 'rb') as f:
                data = pickle.load(f)
                self.node_to_shard = data['partition_assignment']

            if config.VERBOSE:
                print(f"✓ Loaded partition data: {len(self.node_to_shard)} nodes")
        except Exception as e:
            print(f"✗ Failed to load partition data: {e}")
            raise

    def record_access(self,
                     node_id: int,
                     accessing_shard: int,
                     latency_ms: float,
                     is_proxy: bool = False):
        """
        Record a node access from a specific shard.

        Args:
            node_id: ID of the node being accessed
            accessing_shard: Which shard is making the query
            latency_ms: Query latency in milliseconds
            is_proxy: Whether this access went through a proxy
        """
        self.query_count += 1
        self.total_latency += latency_ms

        # Get home shard for this node
        home_shard = self.node_to_shard.get(node_id, accessing_shard)

        # Track cross-shard vs local
        is_cross_shard = (home_shard != accessing_shard)
        if is_cross_shard:
            self.cross_shard_queries += 1
        else:
            self.local_queries += 1

        # Initialize or update stats for this node
        if node_id not in self.proxy_stats:
            self.proxy_stats[node_id] = ProxyAccessStats(
                node_id=node_id,
                home_shard=home_shard
            )

        stats = self.proxy_stats[node_id]
        stats.total_accesses += 1
        stats.access_by_shard[accessing_shard] += 1
        stats.total_latency += latency_ms
        stats.latency_history.append(latency_ms)
        stats.last_accessed = time.time()

        if is_cross_shard:
            stats.cross_shard_accesses += 1
        else:
            stats.local_accesses += 1

        # Add to query history for pattern detection
        self.query_history.append({
            'node_id': node_id,
            'accessing_shard': accessing_shard,
            'home_shard': home_shard,
            'latency_ms': latency_ms,
            'timestamp': time.time(),
            'is_cross_shard': is_cross_shard
        })

    def get_hot_nodes(self, percentile: int = 90) -> List[Tuple[int, ProxyAccessStats]]:
        """
        Get nodes in the top percentile of access frequency.

        Args:
            percentile: Percentile threshold (e.g., 90 = top 10%)

        Returns:
            List of (node_id, stats) tuples sorted by access frequency
        """
        if not self.proxy_stats:
            return []

        # Sort by access frequency
        sorted_stats = sorted(
            self.proxy_stats.items(),
            key=lambda x: x[1].access_frequency,
            reverse=True
        )

        # Get top percentile
        cutoff_index = max(1, int(len(sorted_stats) * (1 - percentile / 100)))
        return sorted_stats[:cutoff_index]

    def get_cross_shard_heavy_nodes(self, min_ratio: float = 0.5) -> List[Tuple[int, ProxyAccessStats]]:
        """
        Get nodes with high cross-shard access ratio.

        Args:
            min_ratio: Minimum cross-shard ratio (0.5 = 50% cross-shard)

        Returns:
            List of (node_id, stats) tuples
        """
        result = [
            (node_id, stats)
            for node_id, stats in self.proxy_stats.items()
            if stats.cross_shard_ratio >= min_ratio and stats.total_accesses >= config.MIN_QUERIES_THRESHOLD
        ]

        # Sort by cross-shard access count
        result.sort(key=lambda x: x[1].cross_shard_accesses, reverse=True)
        return result

    def get_replication_candidates(self, shard_id: int, top_k: int = 10) -> List[Tuple[int, ProxyAccessStats]]:
        """
        Get top candidates for replication to a specific shard.

        These are nodes frequently accessed FROM this shard but living elsewhere.

        Args:
            shard_id: Target shard for replication
            top_k: Number of candidates to return

        Returns:
            List of (node_id, stats) tuples
        """
        candidates = [
            (node_id, stats)
            for node_id, stats in self.proxy_stats.items()
            if stats.home_shard != shard_id  # Node lives elsewhere
            and stats.access_by_shard[shard_id] >= config.MIN_QUERIES_THRESHOLD  # Frequently accessed from this shard
        ]

        # Sort by access frequency from this shard
        candidates.sort(key=lambda x: x[1].access_by_shard[shard_id], reverse=True)
        return candidates[:top_k]

    def get_statistics(self) -> Dict:
        """Get overall monitoring statistics"""
        runtime = time.time() - self.start_time

        return {
            'total_queries': self.query_count,
            'runtime_seconds': runtime,
            'queries_per_second': self.query_count / max(runtime, 1.0),
            'avg_latency_ms': self.total_latency / max(self.query_count, 1),
            'cross_shard_queries': self.cross_shard_queries,
            'local_queries': self.local_queries,
            'cross_shard_percentage': (self.cross_shard_queries / max(self.query_count, 1)) * 100,
            'unique_nodes_accessed': len(self.proxy_stats),
            'hot_nodes_count': len(self.get_hot_nodes()),
        }

    def print_summary(self):
        """Print a summary of monitoring statistics"""
        stats = self.get_statistics()

        print("\n" + "=" * 70)
        print("QUERY MONITORING SUMMARY")
        print("=" * 70)
        print(f"Total Queries:           {stats['total_queries']}")
        print(f"Runtime:                 {stats['runtime_seconds']:.1f} seconds")
        print(f"Queries/second:          {stats['queries_per_second']:.2f}")
        print(f"Average Latency:         {stats['avg_latency_ms']:.2f} ms")
        print(f"Cross-shard Queries:     {stats['cross_shard_queries']} ({stats['cross_shard_percentage']:.1f}%)")
        print(f"Local Queries:           {stats['local_queries']}")
        print(f"Unique Nodes Accessed:   {stats['unique_nodes_accessed']}")
        print(f"Hot Nodes (top 10%):     {stats['hot_nodes_count']}")
        print("=" * 70)

    def save_to_file(self, filepath: str = None):
        """Save monitoring data to file"""
        if filepath is None:
            filepath = config.MONITORING_DATA_PATH

        data = {
            'proxy_stats': self.proxy_stats,
            'query_count': self.query_count,
            'start_time': self.start_time,
            'node_to_shard': self.node_to_shard,
            'query_history': list(self.query_history),
            'statistics': self.get_statistics()
        }

        with open(filepath, 'wb') as f:
            pickle.dump(data, f)

        if config.VERBOSE:
            print(f"✓ Monitoring data saved to {filepath}")

    def load_from_file(self, filepath: str = None):
        """Load monitoring data from file"""
        if filepath is None:
            filepath = config.MONITORING_DATA_PATH

        with open(filepath, 'rb') as f:
            data = pickle.load(f)

        self.proxy_stats = data['proxy_stats']
        self.query_count = data['query_count']
        self.start_time = data['start_time']
        self.node_to_shard = data['node_to_shard']
        self.query_history = deque(data['query_history'], maxlen=config.PATTERN_DETECTION_WINDOW)

        if config.VERBOSE:
            print(f"✓ Monitoring data loaded from {filepath}")

    def detect_access_patterns(self) -> Dict:
        """
        Analyze query history to detect temporal patterns.

        Returns:
            Dictionary with pattern analysis results
        """
        if not self.query_history:
            return {}

        # Calculate access patterns per node
        node_access_times = defaultdict(list)
        for query in self.query_history:
            node_access_times[query['node_id']].append(query['timestamp'])

        # Detect bursty vs steady access patterns
        patterns = {}
        for node_id, timestamps in node_access_times.items():
            if len(timestamps) < 3:
                continue

            # Calculate inter-arrival times
            intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
            avg_interval = sum(intervals) / len(intervals)
            std_interval = (sum((x - avg_interval) ** 2 for x in intervals) / len(intervals)) ** 0.5

            # Coefficient of variation
            cv = std_interval / avg_interval if avg_interval > 0 else 0

            # Classify pattern
            if cv < 0.5:
                pattern_type = 'steady'
            elif cv < 1.5:
                pattern_type = 'moderate'
            else:
                pattern_type = 'bursty'

            patterns[node_id] = {
                'type': pattern_type,
                'avg_interval': avg_interval,
                'cv': cv,
                'access_count': len(timestamps)
            }

        return patterns

    def reset(self):
        """Reset all monitoring statistics"""
        self.proxy_stats.clear()
        self.query_count = 0
        self.start_time = time.time()
        self.query_history.clear()
        self.total_latency = 0.0
        self.cross_shard_queries = 0
        self.local_queries = 0

        if config.VERBOSE:
            print("✓ QueryMonitor reset")


if __name__ == "__main__":
    # Example usage and testing
    print("Testing QueryMonitor...")

    monitor = QueryMonitor()

    # Simulate loading partition data
    monitor.node_to_shard = {
        0: 0, 1: 0, 2: 0,  # Nodes 0-2 in shard 0
        3: 1, 4: 1, 5: 1,  # Nodes 3-5 in shard 1
        6: 2, 7: 2, 8: 2,  # Nodes 6-8 in shard 2
    }

    # Simulate some queries
    # Node 3 (in shard 1) accessed frequently from shard 0
    for i in range(50):
        monitor.record_access(node_id=3, accessing_shard=0, latency_ms=15.0, is_proxy=True)

    # Node 7 (in shard 2) accessed from shard 0 and 1
    for i in range(30):
        monitor.record_access(node_id=7, accessing_shard=0, latency_ms=20.0, is_proxy=True)
        monitor.record_access(node_id=7, accessing_shard=1, latency_ms=18.0, is_proxy=True)

    # Local accesses
    for i in range(100):
        monitor.record_access(node_id=0, accessing_shard=0, latency_ms=5.0, is_proxy=False)

    # Print results
    monitor.print_summary()

    print("\nHot Nodes:")
    for node_id, stats in monitor.get_hot_nodes(percentile=80):
        print(f"  Node {node_id}: {stats.total_accesses} accesses, "
              f"{stats.access_frequency:.2f} q/min, "
              f"{stats.cross_shard_ratio:.2%} cross-shard")

    print("\nReplication Candidates for Shard 0:")
    for node_id, stats in monitor.get_replication_candidates(shard_id=0, top_k=5):
        print(f"  Node {node_id}: {stats.access_by_shard[0]} accesses from shard 0, "
              f"home shard: {stats.home_shard}")
