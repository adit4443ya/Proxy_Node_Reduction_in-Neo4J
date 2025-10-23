"""
Dynamic Proxy Node Manager

This module implements the core logic for dynamically promoting, replicating,
and managing proxy nodes based on query patterns and scoring.
"""

from typing import Dict, List, Set, Tuple
import pickle
from neo4j import GraphDatabase
from dataclasses import dataclass
from collections import defaultdict
import networkx as nx

from query_monitor import QueryMonitor
from proxy_scorer import ProxyScorer, ProxyScore
import config


@dataclass
class ReplicationAction:
    """Represents a replication action to be executed"""
    node_id: int
    source_shard: int  # Where node currently lives
    target_shard: int  # Where to replicate
    action_type: str   # 'promote', 'replicate', 'demote'
    score: float
    timestamp: float = 0.0

    def __repr__(self):
        return (f"ReplicationAction({self.action_type}: node {self.node_id} "
                f"from shard {self.source_shard} to shard {self.target_shard}, "
                f"score={self.score:.2f})")


class DynamicProxyManager:
    """
    Manages dynamic proxy node promotion and replication.

    Responsibilities:
    1. Evaluate proxy scores periodically
    2. Decide which proxies to promote/replicate
    3. Execute Neo4j operations to perform promotions
    4. Track replication budget and constraints
    5. Maintain history of optimizations
    """

    def __init__(self, monitor: QueryMonitor, scorer: ProxyScorer):
        """
        Initialize the dynamic proxy manager.

        Args:
            monitor: QueryMonitor instance
            scorer: ProxyScorer instance
        """
        self.monitor = monitor
        self.scorer = scorer
        self.driver = None

        # Track current replications
        self.replicated_nodes: Dict[int, Set[int]] = defaultdict(set)  # node_id -> set of shards
        self.replication_counts_per_shard: Dict[int, int] = defaultdict(int)

        # Track optimization history
        self.action_history: List[ReplicationAction] = []
        self.evaluation_count = 0

        # Statistics
        self.promotions_executed = 0
        self.replications_executed = 0
        self.demotions_executed = 0
        self.total_latency_saved = 0.0

        if config.VERBOSE:
            print("✓ DynamicProxyManager initialized")

    def connect_to_neo4j(self):
        """Establish Neo4j connection"""
        self.driver = GraphDatabase.driver(
            config.NEO4J_URI,
            auth=(config.NEO4J_USER, config.NEO4J_PASSWORD)
        )
        if config.VERBOSE:
            print(f"✓ Connected to Neo4j at {config.NEO4J_URI}")

    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            if config.VERBOSE:
                print("✓ Neo4j connection closed")

    def get_node_data_from_home_shard(self, node_id: int, home_shard: int) -> Dict:
        """
        Fetch full node data from its home shard.

        Args:
            node_id: Node ID to fetch
            home_shard: Shard where the node lives as Person

        Returns:
            Dictionary with node properties
        """
        db_name = config.SHARD_DATABASES[home_shard]

        with self.driver.session(database=db_name) as session:
            query = """
            MATCH (p:Person {id: $node_id})
            RETURN p.id as id, p.name as name, p.age as age,
                   p.region as region, p.tenant_id as tenant_id
            """
            result = session.run(query, node_id=node_id)
            record = result.single()

            if record:
                return dict(record)
            else:
                return None

    def promote_proxy_to_person(self, node_id: int, target_shard: int, home_shard: int):
        """
        Promote a PersonProxy to a full Person node in target shard.

        Steps:
        1. Fetch full node data from home shard
        2. Delete PersonProxy in target shard (if exists)
        3. Create full Person node with all properties
        4. Recreate edges

        Args:
            node_id: Node to promote
            target_shard: Shard where promotion happens
            home_shard: Original shard with full data
        """
        # Get full node data
        node_data = self.get_node_data_from_home_shard(node_id, home_shard)

        if node_data is None:
            print(f"✗ Failed to fetch data for node {node_id} from shard {home_shard}")
            return False

        db_name = config.SHARD_DATABASES[target_shard]

        with self.driver.session(database=db_name) as session:
            # Delete existing proxy (if any)
            session.run("MATCH (p:PersonProxy {id: $node_id}) DELETE p", node_id=node_id)

            # Create full Person node
            create_query = """
            CREATE (p:Person {
                id: $id,
                name: $name,
                age: $age,
                region: $region,
                tenant_id: $tenant_id
            })
            """
            session.run(create_query, **node_data)

            # Note: Edges will be recreated by fetching connections from original shard
            # For simplicity, we keep the existing edges that pointed to the proxy

        if config.VERBOSE:
            print(f"  ✓ Promoted node {node_id} to Person in shard {target_shard}")

        return True

    def replicate_node_to_shard(self, node_id: int, target_shard: int, home_shard: int):
        """
        Replicate a node to another shard (alias for promote).

        This is essentially the same as promotion - converting a proxy to full node.

        Args:
            node_id: Node to replicate
            target_shard: Target shard
            home_shard: Source shard
        """
        return self.promote_proxy_to_person(node_id, target_shard, home_shard)

    def check_replication_budget(self, shard_id: int) -> bool:
        """
        Check if shard has budget for more replications.

        Args:
            shard_id: Shard to check

        Returns:
            True if budget available, False otherwise
        """
        # Get total nodes in shard
        db_name = config.SHARD_DATABASES[shard_id]

        with self.driver.session(database=db_name) as session:
            result = session.run("MATCH (p:Person) RETURN count(p) as count")
            person_count = result.single()['count']

        # Calculate current replication percentage
        current_percentage = self.replication_counts_per_shard[shard_id] / max(person_count, 1)

        return current_percentage < config.MAX_REPLICATION_PERCENTAGE

    def execute_greedy_strategy(self) -> List[ReplicationAction]:
        """
        Execute greedy top-K strategy.

        For each shard, promote top K scoring proxies.

        Returns:
            List of actions executed
        """
        actions = []
        top_scores = self.scorer.get_top_k_per_shard(k=config.GREEDY_TOP_K)

        for target_shard, scores in top_scores.items():
            # Check budget
            if not self.check_replication_budget(target_shard):
                if config.VERBOSE:
                    print(f"  ! Shard {target_shard} at replication budget limit")
                continue

            for score in scores:
                # Execute promotion
                if config.ENABLE_PROMOTION or config.ENABLE_REPLICATION:
                    success = self.promote_proxy_to_person(
                        score.node_id,
                        target_shard,
                        score.home_shard
                    )

                    if success:
                        action = ReplicationAction(
                            node_id=score.node_id,
                            source_shard=score.home_shard,
                            target_shard=target_shard,
                            action_type='promote',
                            score=score.score,
                            timestamp=self.monitor.query_count
                        )
                        actions.append(action)
                        self.replicated_nodes[score.node_id].add(target_shard)
                        self.replication_counts_per_shard[target_shard] += 1
                        self.promotions_executed += 1

                        # Check if we've hit budget
                        if not self.check_replication_budget(target_shard):
                            break

        return actions

    def execute_threshold_strategy(self) -> List[ReplicationAction]:
        """
        Execute threshold-based strategy.

        Promote proxies with scores above threshold.

        Returns:
            List of actions executed
        """
        actions = []
        threshold_scores = self.scorer.get_scores_above_threshold(config.PROMOTION_THRESHOLD)

        for target_shard, scores in threshold_scores.items():
            # Check budget
            if not self.check_replication_budget(target_shard):
                if config.VERBOSE:
                    print(f"  ! Shard {target_shard} at replication budget limit")
                continue

            for score in scores:
                # Execute promotion
                if config.ENABLE_PROMOTION or config.ENABLE_REPLICATION:
                    success = self.promote_proxy_to_person(
                        score.node_id,
                        target_shard,
                        score.home_shard
                    )

                    if success:
                        action = ReplicationAction(
                            node_id=score.node_id,
                            source_shard=score.home_shard,
                            target_shard=target_shard,
                            action_type='promote',
                            score=score.score,
                            timestamp=self.monitor.query_count
                        )
                        actions.append(action)
                        self.replicated_nodes[score.node_id].add(target_shard)
                        self.replication_counts_per_shard[target_shard] += 1
                        self.promotions_executed += 1

                        # Check budget after each action
                        if not self.check_replication_budget(target_shard):
                            break

        return actions

    def execute_budget_aware_strategy(self) -> List[ReplicationAction]:
        """
        Execute budget-aware strategy.

        Use greedy selection within budget constraints.
        When at capacity, evict lowest-scored replications.

        Returns:
            List of actions executed
        """
        actions = []
        all_scores = self.scorer.score_all_proxies()

        for target_shard, scores in all_scores.items():
            if not scores:
                continue

            # Get shard statistics
            db_name = config.SHARD_DATABASES[target_shard]
            with self.driver.session(database=db_name) as session:
                result = session.run("MATCH (p:Person) RETURN count(p) as count")
                person_count = result.single()['count']

            # Calculate budget
            max_replications = int(person_count * config.MAX_REPLICATION_PERCENTAGE)
            current_replications = self.replication_counts_per_shard[target_shard]
            available_budget = max_replications - current_replications

            if available_budget <= 0:
                if config.VERBOSE:
                    print(f"  ! Shard {target_shard} at budget limit "
                          f"({current_replications}/{max_replications})")
                continue

            # Take top scores within budget
            actions_for_shard = scores[:available_budget]

            for score in actions_for_shard:
                if config.ENABLE_PROMOTION or config.ENABLE_REPLICATION:
                    success = self.promote_proxy_to_person(
                        score.node_id,
                        target_shard,
                        score.home_shard
                    )

                    if success:
                        action = ReplicationAction(
                            node_id=score.node_id,
                            source_shard=score.home_shard,
                            target_shard=target_shard,
                            action_type='promote',
                            score=score.score,
                            timestamp=self.monitor.query_count
                        )
                        actions.append(action)
                        self.replicated_nodes[score.node_id].add(target_shard)
                        self.replication_counts_per_shard[target_shard] += 1
                        self.promotions_executed += 1

        return actions

    def evaluate_and_optimize(self) -> List[ReplicationAction]:
        """
        Main evaluation loop - score proxies and execute optimization strategy.

        Returns:
            List of actions taken
        """
        self.evaluation_count += 1

        if config.VERBOSE:
            print(f"\n{'='*70}")
            print(f"OPTIMIZATION EVALUATION #{self.evaluation_count}")
            print(f"Query count: {self.monitor.query_count}")
            print(f"{'='*70}")

        # Execute strategy
        if config.MANAGEMENT_STRATEGY == 'greedy':
            actions = self.execute_greedy_strategy()
        elif config.MANAGEMENT_STRATEGY == 'threshold':
            actions = self.execute_threshold_strategy()
        elif config.MANAGEMENT_STRATEGY == 'budget_aware':
            actions = self.execute_budget_aware_strategy()
        else:
            print(f"✗ Unknown strategy: {config.MANAGEMENT_STRATEGY}")
            return []

        # Record actions
        self.action_history.extend(actions)

        if config.VERBOSE:
            print(f"\n✓ Executed {len(actions)} optimization actions")
            for action in actions:
                print(f"  - {action}")

        return actions

    def get_statistics(self) -> Dict:
        """Get optimization statistics"""
        return {
            'evaluation_count': self.evaluation_count,
            'promotions_executed': self.promotions_executed,
            'replications_executed': self.replications_executed,
            'demotions_executed': self.demotions_executed,
            'total_actions': len(self.action_history),
            'unique_nodes_replicated': len(self.replicated_nodes),
            'replications_per_shard': dict(self.replication_counts_per_shard),
        }

    def print_statistics(self):
        """Print optimization statistics"""
        stats = self.get_statistics()

        print("\n" + "=" * 70)
        print("DYNAMIC PROXY MANAGEMENT STATISTICS")
        print("=" * 70)
        print(f"Evaluations Performed:   {stats['evaluation_count']}")
        print(f"Promotions Executed:     {stats['promotions_executed']}")
        print(f"Replications Executed:   {stats['replications_executed']}")
        print(f"Demotions Executed:      {stats['demotions_executed']}")
        print(f"Total Actions:           {stats['total_actions']}")
        print(f"Unique Nodes Replicated: {stats['unique_nodes_replicated']}")
        print(f"\nReplications per Shard:")
        for shard_id, count in stats['replications_per_shard'].items():
            print(f"  Shard {shard_id}: {count}")
        print("=" * 70)

    def save_history(self, filepath: str = None):
        """Save optimization history to file"""
        if filepath is None:
            filepath = config.OPTIMIZATION_HISTORY_PATH

        data = {
            'action_history': self.action_history,
            'replicated_nodes': dict(self.replicated_nodes),
            'replication_counts': dict(self.replication_counts_per_shard),
            'statistics': self.get_statistics()
        }

        with open(filepath, 'wb') as f:
            pickle.dump(data, f)

        if config.VERBOSE:
            print(f"✓ Optimization history saved to {filepath}")


if __name__ == "__main__":
    # Example usage
    print("Testing DynamicProxyManager...")

    from query_monitor import QueryMonitor
    from proxy_scorer import ProxyScorer

    # Create components
    monitor = QueryMonitor()
    monitor.node_to_shard = {i: i % 3 for i in range(20)}

    # Simulate queries
    for _ in range(50):
        monitor.record_access(node_id=5, accessing_shard=0, latency_ms=15.0, is_proxy=True)
    for _ in range(30):
        monitor.record_access(node_id=8, accessing_shard=1, latency_ms=18.0, is_proxy=True)

    scorer = ProxyScorer(monitor)

    # Create manager
    manager = DynamicProxyManager(monitor, scorer)

    print("\nCurrent configuration:")
    print(f"  Strategy: {config.MANAGEMENT_STRATEGY}")
    print(f"  Evaluation frequency: every {config.EVALUATION_FREQUENCY} queries")
    print(f"  Max replication: {config.MAX_REPLICATION_PERCENTAGE * 100}%")

    print("\n✓ DynamicProxyManager test completed")
    print("  (Neo4j operations skipped in test mode)")
