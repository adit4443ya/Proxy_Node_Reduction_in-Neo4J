import pickle
import networkx as nx
from neo4j import GraphDatabase

class Neo4jShardLoader:
    def __init__(self, partition_assignment, graph):
        self.partition = partition_assignment
        self.graph = graph
        self.uri = "bolt://localhost:7687"
        self.auth = ("neo4j", "password123")
        
        # Database names (all in same instance)
        self.db_names = {
            0: "shard1",
            1: "shard2",
            2: "shard3"
        }
    
    def clear_database(self, db_name):
        """Clear all data from a database"""
        driver = GraphDatabase.driver(self.uri, auth=self.auth)
        with driver.session(database=db_name) as session:
            session.run("MATCH (n) DETACH DELETE n")
        driver.close()
        print(f"Cleared database: {db_name}")
    
    def load_partition_to_database(self, partition_id):
        """Load a partition into its database"""
        db_name = self.db_names[partition_id]
        driver = GraphDatabase.driver(self.uri, auth=self.auth)
        
        # Get nodes in this partition
        partition_nodes = [node for node, part in self.partition.items() 
                          if part == partition_id]
        
        print(f"\nLoading {len(partition_nodes)} nodes to database '{db_name}'...")
        
        with driver.session(database=db_name) as session:
            # Create nodes in batches
            batch_size = 1000
            for i in range(0, len(partition_nodes), batch_size):
                batch = partition_nodes[i:i+batch_size]
                
                for node_id in batch:
                    attrs = self.graph.nodes[node_id]
                    session.run("""
                        CREATE (p:Person {
                            id: $id,
                            name: $name,
                            age: $age,
                            region: $region,
                            tenant_id: $tenant_id
                        })
                    """, id=node_id, **attrs)
                
                print(f"  Created {min(i+batch_size, len(partition_nodes))}/{len(partition_nodes)} nodes")
            
            # Create relationships (within database only)
            edge_count = 0
            proxy_needed = set()
            
            for u, v in self.graph.edges():
                part_u = self.partition[u]
                part_v = self.partition[v]
                
                # Both nodes in this partition - create actual relationship
                if part_u == partition_id and part_v == partition_id:
                    session.run("""
                        MATCH (u:Person {id: $u_id})
                        MATCH (v:Person {id: $v_id})
                        MERGE (u)-[:KNOWS]->(v)
                    """, u_id=u, v_id=v)
                    edge_count += 1
                
                # Need proxy: u is here, v is in another partition
                elif part_u == partition_id and part_v != partition_id:
                    proxy_needed.add(v)
                
                # Need proxy: v is here, u is in another partition  
                elif part_v == partition_id and part_u != partition_id:
                    proxy_needed.add(u)
            
            print(f"  Created {edge_count} internal relationships")
            
            # Create proxy nodes
            for proxy_id in proxy_needed:
                session.run("""
                    CREATE (p:PersonProxy {id: $id})
                """, id=proxy_id)
            
            # Create relationships to proxies
            proxy_edge_count = 0
            for u, v in self.graph.edges():
                part_u = self.partition[u]
                part_v = self.partition[v]
                
                # u in this partition, v needs proxy
                if part_u == partition_id and part_v != partition_id:
                    session.run("""
                        MATCH (u:Person {id: $u_id})
                        MATCH (p:PersonProxy {id: $proxy_id})
                        MERGE (u)-[:KNOWS]->(p)
                    """, u_id=u, proxy_id=v)
                    proxy_edge_count += 1
                
                # v in this partition, u needs proxy
                elif part_v == partition_id and part_u != partition_id:
                    session.run("""
                        MATCH (v:Person {id: $v_id})
                        MATCH (p:PersonProxy {id: $proxy_id})
                        MERGE (v)-[:KNOWS]->(p)
                    """, v_id=v, proxy_id=u)
                    proxy_edge_count += 1
            
            print(f"  Created {len(proxy_needed)} proxy nodes")
            print(f"  Created {proxy_edge_count} proxy relationships")
        
        driver.close()

# Load data
with open('../data/graph.pkl', 'rb') as f:
    graph = pickle.load(f)['graph']

with open('../data/refined_partition.pkl', 'rb') as f:
    partition = pickle.load(f)['partition_assignment']

# Load to Neo4j
loader = Neo4jShardLoader(partition, graph)

for partition_id in range(3):
    db_name = loader.db_names[partition_id]
    loader.clear_database(db_name)
    loader.load_partition_to_database(partition_id)

print("\n✓ All databases loaded!")