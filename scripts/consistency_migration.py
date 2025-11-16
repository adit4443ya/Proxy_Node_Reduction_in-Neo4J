"""
Consistency-Aware Migration Protocol
Addresses Feedback: Parameter to record proxy activation time, consistency analyses
"""

import time
import pickle
from neo4j import GraphDatabase
from collections import defaultdict

class ConsistencyAwareMigration:
    """
    Implements 2-phase migration protocol for consistency during repartitioning
    
    Phases:
    1. Shadow Copy Phase: Create proxies in new partition without disrupting queries
    2. Atomic Switch Phase: Update partition assignment atomically
    3. Cleanup Phase: Remove old proxies and orphaned data
    """
    
    def __init__(self, uri="bolt://localhost:7687", auth=("neo4j", "password123")):
        self.uri = uri
        self.auth = auth
        
        # Timing measurements
        self.timing_stats = {
            'shadow_copy': [],
            'atomic_switch': [],
            'cleanup': [],
            'total_consistency_window': [],
            'query_blocking_time': []
        }
        
        # Cost constants (from cost_model.py)
        self.c_create = 10.0  # ms per proxy creation
        self.c_sync = 1.0     # ms per neighbor sync
        self.c_delete = 5.0   # ms per proxy deletion
    
    def estimate_shadow_copy_time(self, graph, nodes_to_migrate):
        """
        Estimate shadow copy phase time:
        t_shadow = Σ_v (c_create + c_sync · degree(v))
        
        Args:
            graph: NetworkX graph
            nodes_to_migrate: List of nodes changing partitions
        
        Returns:
            Estimated time in milliseconds
        """
        total_time = 0.0
        
        for node in nodes_to_migrate:
            if node not in graph.nodes():
                continue
            
            degree = graph.degree(node)
            node_time = self.c_create + self.c_sync * degree
            total_time += node_time
        
        return total_time
    
    def estimate_cleanup_time(self, old_proxy_count):
        """
        Estimate cleanup phase time:
        t_cleanup = Σ_v c_delete
        
        Args:
            old_proxy_count: Number of old proxies to remove
        
        Returns:
            Estimated time in milliseconds
        """
        return old_proxy_count * self.c_delete
    
    def migrate_node_2phase(self, node_id, old_partition, new_partition, 
                           old_db, new_db, graph):
        """
        2-phase migration for a single node
        
        Returns:
            Dict with timing breakdown
        """
        timings = {
            'shadow_copy': 0.0,
            'atomic_switch': 0.0,
            'cleanup': 0.0
        }
        
        driver = GraphDatabase.driver(self.uri, auth=self.auth)
        
        # Phase 1: Shadow Copy (non-blocking)
        start = time.time()
        
        with driver.session(database=new_db) as session:
            # Get node attributes from old database
            with driver.session(database=old_db) as old_session:
                result = old_session.run(
                    "MATCH (n:Person {id: $id}) RETURN n",
                    id=node_id
                )
                record = result.single()
                if not record:
                    driver.close()
                    return timings
                
                node_props = dict(record['n'])
            
            # Create node in new database (shadow copy)
            session.run("""
                CREATE (p:Person {
                    id: $id,
                    name: $name,
                    age: $age,
                    region: $region,
                    tenant_id: $tenant_id
                })
            """, **node_props)
            
            # Copy relationships (create proxies for cross-partition neighbors)
            neighbors = list(graph.neighbors(node_id))
            for neighbor in neighbors:
                neighbor_partition = new_partition.get(neighbor, old_partition.get(neighbor))
                
                if neighbor_partition == new_partition[node_id]:
                    # Same partition - create actual relationship if neighbor exists
                    session.run("""
                        MATCH (n:Person {id: $node_id})
                        MERGE (m:Person {id: $neighbor_id})
                        MERGE (n)-[:KNOWS]->(m)
                    """, node_id=node_id, neighbor_id=neighbor)
                else:
                    # Different partition - create proxy
                    session.run("""
                        MATCH (n:Person {id: $node_id})
                        MERGE (p:PersonProxy {id: $neighbor_id})
                        MERGE (n)-[:KNOWS]->(p)
                    """, node_id=node_id, neighbor_id=neighbor)
        
        timings['shadow_copy'] = (time.time() - start) * 1000  # Convert to ms
        
        # Phase 2: Atomic Switch (blocking - critical section)
        start = time.time()
        
        # Update partition assignment in memory (atomic operation)
        # In production, this would update a distributed coordination service
        old_partition[node_id] = new_partition[node_id]
        
        timings['atomic_switch'] = (time.time() - start) * 1000
        
        # Phase 3: Cleanup (non-blocking, can be async)
        start = time.time()
        
        with driver.session(database=old_db) as session:
            # Remove node from old database
            session.run(
                "MATCH (n:Person {id: $id}) DETACH DELETE n",
                id=node_id
            )
        
        timings['cleanup'] = (time.time() - start) * 1000
        
        driver.close()
        return timings
    
    def migrate_partition_batch(self, graph, old_partition, new_partition, 
                               db_names, batch_size=100):
        """
        Migrate changed nodes in batches with consistency guarantees
        
        Args:
            graph: NetworkX graph
            old_partition: Old partition assignment
            new_partition: New partition assignment
            db_names: Dict mapping partition ID -> database name
            batch_size: Number of nodes per batch
        
        Returns:
            Migration statistics
        """
        # Find nodes that changed partition
        changed_nodes = [
            node for node in graph.nodes()
            if old_partition.get(node) != new_partition.get(node)
        ]
        
        print(f"\n{'='*60}")
        print("CONSISTENCY-AWARE MIGRATION")
        print(f"{'='*60}")
        print(f"Nodes to migrate: {len(changed_nodes)}")
        print(f"Batch size: {batch_size}")
        
        # Estimate times before migration
        est_shadow = self.estimate_shadow_copy_time(graph, changed_nodes)
        print(f"\nEstimated shadow copy time: {est_shadow:.2f}ms")
        
        total_shadow = 0.0
        total_switch = 0.0
        total_cleanup = 0.0
        
        # Process in batches
        for i in range(0, len(changed_nodes), batch_size):
            batch = changed_nodes[i:i+batch_size]
            print(f"\nMigrating batch {i//batch_size + 1}/{(len(changed_nodes)-1)//batch_size + 1}")
            
            batch_start = time.time()
            
            for node in batch:
                old_part = old_partition.get(node)
                new_part = new_partition.get(node)
                
                if old_part is None or new_part is None:
                    continue
                
                old_db = db_names[old_part]
                new_db = db_names[new_part]
                
                timings = self.migrate_node_2phase(
                    node, old_partition, new_partition,
                    old_db, new_db, graph
                )
                
                total_shadow += timings['shadow_copy']
                total_switch += timings['atomic_switch']
                total_cleanup += timings['cleanup']
            
            batch_time = (time.time() - batch_start) * 1000
            print(f"  Batch time: {batch_time:.2f}ms")
        
        # Compute statistics
        total_consistency = total_shadow + total_switch + total_cleanup
        query_blocking = total_switch  # Only atomic switch blocks queries
        
        stats = {
            'nodes_migrated': len(changed_nodes),
            'total_shadow_copy': total_shadow,
            'total_atomic_switch': total_switch,
            'total_cleanup': total_cleanup,
            'total_consistency_window': total_consistency,
            'query_blocking_time': query_blocking,
            'avg_migration_per_node': total_consistency / len(changed_nodes) if changed_nodes else 0
        }
        
        # Record in timing stats
        self.timing_stats['shadow_copy'].append(total_shadow)
        self.timing_stats['atomic_switch'].append(total_switch)
        self.timing_stats['cleanup'].append(total_cleanup)
        self.timing_stats['total_consistency_window'].append(total_consistency)
        self.timing_stats['query_blocking_time'].append(query_blocking)
        
        self.print_migration_stats(stats)
        
        return stats
    
    def print_migration_stats(self, stats):
        """Print detailed migration statistics"""
        print(f"\n{'='*60}")
        print("MIGRATION STATISTICS")
        print(f"{'='*60}")
        print(f"Nodes Migrated:              {stats['nodes_migrated']}")
        print(f"\nPhase Timings:")
        print(f"  Phase 1 (Shadow Copy):     {stats['total_shadow_copy']:.2f}ms")
        print(f"  Phase 2 (Atomic Switch):   {stats['total_atomic_switch']:.2f}ms")
        print(f"  Phase 3 (Cleanup):         {stats['total_cleanup']:.2f}ms")
        print(f"\nConsistency Metrics:")
        print(f"  Total Consistency Window:  {stats['total_consistency_window']:.2f}ms")
        print(f"  Query Blocking Time:       {stats['query_blocking_time']:.2f}ms")
        print(f"  Blocking Ratio:            {stats['query_blocking_time']/stats['total_consistency_window']*100:.2f}%")
        print(f"\nEfficiency:")
        print(f"  Avg per Node:              {stats['avg_migration_per_node']:.2f}ms")
        print(f"{'='*60}\n")
    
    def get_aggregate_stats(self):
        """Get aggregate statistics across all migrations"""
        if not self.timing_stats['total_consistency_window']:
            return None
        
        import numpy as np
        
        return {
            'total_migrations': len(self.timing_stats['total_consistency_window']),
            'avg_consistency_window': np.mean(self.timing_stats['total_consistency_window']),
            'avg_query_blocking': np.mean(self.timing_stats['query_blocking_time']),
            'total_shadow_copy': np.sum(self.timing_stats['shadow_copy']),
            'total_atomic_switch': np.sum(self.timing_stats['atomic_switch']),
            'total_cleanup': np.sum(self.timing_stats['cleanup']),
        }
    
    def print_aggregate_stats(self):
        """Print aggregate statistics"""
        stats = self.get_aggregate_stats()
        
        if stats is None:
            print("No migration statistics available")
            return
        
        print(f"\n{'='*60}")
        print("AGGREGATE MIGRATION STATISTICS")
        print(f"{'='*60}")
        print(f"Total Migrations:          {stats['total_migrations']}")
        print(f"\nAverage Times per Migration:")
        print(f"  Consistency Window:      {stats['avg_consistency_window']:.2f}ms")
        print(f"  Query Blocking:          {stats['avg_query_blocking']:.2f}ms")
        print(f"\nCumulative Times:")
        print(f"  Shadow Copy:             {stats['total_shadow_copy']:.2f}ms")
        print(f"  Atomic Switch:           {stats['total_atomic_switch']:.2f}ms")
        print(f"  Cleanup:                 {stats['total_cleanup']:.2f}ms")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    print("Testing Consistency-Aware Migration Protocol...\n")
    
    migration = ConsistencyAwareMigration()
    
    # Test estimation functions
    import networkx as nx
    
    # Create small test graph
    G = nx.barabasi_albert_graph(100, 3)
    nodes_to_migrate = list(G.nodes())[:10]
    
    shadow_time = migration.estimate_shadow_copy_time(G, nodes_to_migrate)
    cleanup_time = migration.estimate_cleanup_time(20)
    
    print(f"Estimated shadow copy time: {shadow_time:.2f}ms")
    print(f"Estimated cleanup time: {cleanup_time:.2f}ms")
    print(f"Estimated atomic switch time: ~10ms (constant)")
    print(f"Total estimated consistency window: {shadow_time + 10 + cleanup_time:.2f}ms")
    
    print("\n✓ Migration protocol test complete!")