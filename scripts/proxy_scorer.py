"""
Proxy Node Scoring System

This module implements scoring algorithms to determine which proxy nodes
should be promoted or replicated based on access patterns and performance metrics.
"""

from typing import Dict, List, Tuple, Set
from dataclasses import dataclass
import pickle
import networkx as nx
from query_monitor import QueryMonitor, ProxyAccessStats
import config


@dataclass
class ProxyScore:
    """Score and metadata for a proxy replication decision"""
    node_id: int
    target_shard: int  # Shard where replication is proposed
    home_shard: int    # Original shard
    score: float

    # Score components (for debugging/analysis)
    frequency_component: float = 0.0
    cross_shard_component: float = 0.0
    latency_component: float = 0.0
    colocation_component: float = 0.0
    replication_cost: float = 0.0

    # Supporting data
    access_count: int = 0
    avg_latency: float = 0.0
    cross_shard_ratio: float = 0.0

    def __repr__(self):
        return (f"ProxyScore(node={self.node_id}, target_shard={self.target_shard}, "
                f"score={self.score:.2f}, accesses={self.access_count})")


class ProxyScorer:
    """
    Scores proxy nodes to determine replication/promotion priorities.

    The scoring algorithm considers:
    1. Access frequency from the target shard
    2. Cross-shard access ratio (penalty for remote accesses)
    3. Query latency impact
    4. Co-location benefits (neighbors in same shard)
    5. Replication cost (storage and maintenance)
    """

    def __init__(self, monitor: QueryMonitor, graph: nx.Graph = None):
        """
        Initialize scorer.

        Args:
            monitor: QueryMonitor instance with access statistics
            graph: Original graph for analyzing co-location benefits (optional)
        """
        self.monitor = monitor
        self.graph = graph

        # Cache for node degrees (to calculate replication cost)
        self.node_degrees: Dict[int, int] = {}
        if graph is not None:
            self.node_degrees = dict(graph.degree())

        if config.VERBOSE:
            print("✓ ProxyScorer initialized")

    def load_graph(self, graph_file: str = "../data/graph.pkl"):
        """Load the original graph for co-location analysis"""
        try:
            with open(graph_file, 'rb') as f:
                data = pickle.load(f)
                self.graph = data['graph']
                self.node_degrees = dict(self.graph.degree())

            if config.VERBOSE:
                print(f"✓ Loaded graph: {self.graph.number_of_nodes()} nodes, "
                      f"{self.graph.number_of_edges()} edges")
        except Exception as e:
            print(f"✗ Failed to load graph: {e}")

    def calculate_replication_cost(self, node_id: int) -> float:
        """
        Calculate the cost of replicating a node.

        Cost includes:
        - Base cost for storing node data
        - Cost per edge (for maintaining edge data)

        Args:
            node_id: Node to calculate cost for

        Returns:
            Cost value (higher = more expensive)
        """
        degree = self.node_degrees.get(node_id, 0)
        cost = config.REPLICATION_COST_BASE + (degree * config.REPLICATION_COST_PER_EDGE)
        return cost

    def calculate_colocation_benefit(self, node_id: int, target_shard: int) -> float:
        """
        Calculate benefit of co-locating a node with its neighbors.

        Higher benefit when many neighbors already in target shard.

        Args:
            node_id: Node to evaluate
            target_shard: Shard to potentially replicate to

        Returns:
            Benefit score (0-1, higher is better)
        """
        if self.graph is None or node_id not in self.graph:
            return 0.0

        neighbors = list(self.graph.neighbors(node_id))
        if not neighbors:
            return 0.0

        # Count neighbors already in target shard
        neighbors_in_target = sum(
            1 for neighbor_id in neighbors
            if self.monitor.node_to_shard.get(neighbor_id) == target_shard
        )

        # Return ratio of co-located neighbors
        return neighbors_in_target / len(neighbors)

    def score_proxy_for_shard(self, node_id: int, target_shard: int, stats: ProxyAccessStats) -> ProxyScore:
        """
        Calculate score for replicating a specific node to a target shard.

        Scoring formula:
        score = (freq * FREQ_WEIGHT) * (cross_ratio * CROSS_WEIGHT) * (latency * LAT_WEIGHT)
                + (colocation * COLOC_BONUS) - replication_cost

        Args:
            node_id: Node to score
            target_shard: Shard to potentially replicate to
            stats: Access statistics for the node

        Returns:
            ProxyScore object with detailed breakdown
        """
        # Component 1: Access frequency from target shard
        accesses_from_target = stats.access_by_shard[target_shard]
        if accesses_from_target < config.MIN_QUERIES_THRESHOLD:
            # Not enough accesses to justify replication
            return ProxyScore(
                node_id=node_id,
                target_shard=target_shard,
                home_shard=stats.home_shard,
                score=0.0
            )

        # Calculate frequency score (queries per minute from this shard)
        time_span = max(stats.last_accessed - stats.first_seen, 1.0)
        frequency = (accesses_from_target / time_span) * 60.0  # per minute
        frequency_component = frequency * config.FREQUENCY_WEIGHT

        # Component 2: Cross-shard penalty
        # Higher score when most accesses are cross-shard (more benefit from replication)
        cross_shard_component = stats.cross_shard_ratio * config.CROSS_SHARD_WEIGHT

        # Component 3: Latency impact
        # Normalize by baseline and weight
        latency_normalized = stats.avg_latency / config.BASELINE_LATENCY
        latency_component = latency_normalized * config.LATENCY_WEIGHT

        # Component 4: Co-location benefit
        colocation_benefit = self.calculate_colocation_benefit(node_id, target_shard)
        colocation_component = colocation_benefit * config.COLOCATION_BONUS

        # Component 5: Replication cost
        replication_cost = self.calculate_replication_cost(node_id)

        # Calculate final score
        score = (frequency_component * cross_shard_component * latency_component +
                colocation_component - replication_cost)

        return ProxyScore(
            node_id=node_id,
            target_shard=target_shard,
            home_shard=stats.home_shard,
            score=score,
            frequency_component=frequency_component,
            cross_shard_component=cross_shard_component,
            latency_component=latency_component,
            colocation_component=colocation_component,
            replication_cost=replication_cost,
            access_count=accesses_from_target,
            avg_latency=stats.avg_latency,
            cross_shard_ratio=stats.cross_shard_ratio
        )

    def score_all_proxies(self) -> Dict[int, List[ProxyScore]]:
        """
        Score all proxy nodes for all possible target shards.

        Returns:
            Dictionary mapping shard_id -> list of ProxyScores for that shard
            (sorted by score, descending)
        """
        shard_scores: Dict[int, List[ProxyScore]] = {
            shard_id: [] for shard_id in range(len(config.SHARD_DATABASES))
        }

        # For each node with access stats
        for node_id, stats in self.monitor.proxy_stats.items():
            # Skip if below minimum threshold
            if stats.total_accesses < config.MIN_QUERIES_THRESHOLD:
                continue

            # Score for each potential target shard
            for target_shard in range(len(config.SHARD_DATABASES)):
                # Skip home shard (node already there)
                if target_shard == stats.home_shard:
                    continue

                # Calculate score
                proxy_score = self.score_proxy_for_shard(node_id, target_shard, stats)

                # Add to results if score is positive
                if proxy_score.score > 0:
                    shard_scores[target_shard].append(proxy_score)

        # Sort each shard's scores (highest first)
        for shard_id in shard_scores:
            shard_scores[shard_id].sort(key=lambda x: x.score, reverse=True)

        return shard_scores

    def get_top_k_per_shard(self, k: int = 10) -> Dict[int, List[ProxyScore]]:
        """
        Get top K scoring proxy nodes for each shard.

        Args:
            k: Number of top scores to return per shard

        Returns:
            Dictionary mapping shard_id -> top K ProxyScores
        """
        all_scores = self.score_all_proxies()

        return {
            shard_id: scores[:k]
            for shard_id, scores in all_scores.items()
        }

    def get_scores_above_threshold(self, threshold: float = None) -> Dict[int, List[ProxyScore]]:
        """
        Get proxy scores above a threshold for each shard.

        Args:
            threshold: Score threshold (uses config.PROMOTION_THRESHOLD if None)

        Returns:
            Dictionary mapping shard_id -> ProxyScores above threshold
        """
        if threshold is None:
            threshold = config.PROMOTION_THRESHOLD

        all_scores = self.score_all_proxies()

        return {
            shard_id: [score for score in scores if score.score >= threshold]
            for shard_id, scores in all_scores.items()
        }

    def print_top_scores(self, k: int = 5):
        """Print top K scores for each shard"""
        top_scores = self.get_top_k_per_shard(k)

        print("\n" + "=" * 70)
        print("TOP PROXY REPLICATION CANDIDATES")
        print("=" * 70)

        for shard_id, scores in top_scores.items():
            print(f"\n[Shard {shard_id}] Top {len(scores)} candidates:")

            if not scores:
                print("  (none)")
                continue

            for i, score in enumerate(scores, 1):
                print(f"  {i}. Node {score.node_id} (from shard {score.home_shard})")
                print(f"     Score: {score.score:.2f} | Accesses: {score.access_count} | "
                      f"Latency: {score.avg_latency:.1f}ms")
                print(f"     Components: freq={score.frequency_component:.2f}, "
                      f"cross={score.cross_shard_component:.2f}, "
                      f"latency={score.latency_component:.2f}, "
                      f"coloc={score.colocation_component:.2f}, "
                      f"cost={score.replication_cost:.2f}")

        print("=" * 70)

    def analyze_score_distribution(self) -> Dict:
        """Analyze the distribution of proxy scores"""
        all_scores = self.score_all_proxies()

        # Flatten all scores
        all_score_values = [
            score.score
            for scores in all_scores.values()
            for score in scores
        ]

        if not all_score_values:
            return {'error': 'No scores available'}

        all_score_values.sort()

        # Calculate percentiles
        def percentile(data, p):
            index = int(len(data) * p / 100)
            return data[min(index, len(data) - 1)]

        analysis = {
            'total_candidates': len(all_score_values),
            'min_score': min(all_score_values),
            'max_score': max(all_score_values),
            'median_score': percentile(all_score_values, 50),
            'p75_score': percentile(all_score_values, 75),
            'p90_score': percentile(all_score_values, 90),
            'p95_score': percentile(all_score_values, 95),
            'above_threshold': sum(1 for s in all_score_values if s >= config.PROMOTION_THRESHOLD),
        }

        # Per-shard statistics
        analysis['per_shard'] = {}
        for shard_id, scores in all_scores.items():
            if scores:
                analysis['per_shard'][shard_id] = {
                    'candidate_count': len(scores),
                    'top_score': scores[0].score if scores else 0,
                    'avg_score': sum(s.score for s in scores) / len(scores),
                }

        return analysis

    def print_score_analysis(self):
        """Print score distribution analysis"""
        analysis = self.analyze_score_distribution()

        if 'error' in analysis:
            print(f"✗ {analysis['error']}")
            return

        print("\n" + "=" * 70)
        print("PROXY SCORE DISTRIBUTION ANALYSIS")
        print("=" * 70)
        print(f"Total Candidates:        {analysis['total_candidates']}")
        print(f"Score Range:             {analysis['min_score']:.2f} - {analysis['max_score']:.2f}")
        print(f"Median Score:            {analysis['median_score']:.2f}")
        print(f"75th Percentile:         {analysis['p75_score']:.2f}")
        print(f"90th Percentile:         {analysis['p90_score']:.2f}")
        print(f"95th Percentile:         {analysis['p95_score']:.2f}")
        print(f"Above Threshold ({config.PROMOTION_THRESHOLD}): {analysis['above_threshold']}")

        print("\nPer-Shard Statistics:")
        for shard_id, stats in analysis['per_shard'].items():
            print(f"  Shard {shard_id}: {stats['candidate_count']} candidates, "
                  f"top score: {stats['top_score']:.2f}, "
                  f"avg: {stats['avg_score']:.2f}")

        print("=" * 70)


if __name__ == "__main__":
    # Example usage and testing
    print("Testing ProxyScorer...")

    from query_monitor import QueryMonitor

    # Create a monitor with sample data
    monitor = QueryMonitor()
    monitor.node_to_shard = {i: i % 3 for i in range(20)}  # 20 nodes across 3 shards

    # Simulate query patterns
    # Node 5 (shard 2) frequently accessed from shard 0
    for _ in range(100):
        monitor.record_access(node_id=5, accessing_shard=0, latency_ms=15.0, is_proxy=True)

    # Node 8 (shard 2) accessed from multiple shards
    for _ in range(50):
        monitor.record_access(node_id=8, accessing_shard=0, latency_ms=20.0, is_proxy=True)
    for _ in range(40):
        monitor.record_access(node_id=8, accessing_shard=1, latency_ms=18.0, is_proxy=True)

    # Node 12 (shard 0) moderately accessed from shard 1
    for _ in range(30):
        monitor.record_access(node_id=12, accessing_shard=1, latency_ms=12.0, is_proxy=True)

    # Create scorer
    scorer = ProxyScorer(monitor)

    # Test scoring
    print("\nMonitor Summary:")
    monitor.print_summary()

    print("\nScoring Results:")
    scorer.print_top_scores(k=5)

    print("\nScore Distribution:")
    scorer.print_score_analysis()
