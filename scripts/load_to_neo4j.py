import pickle
import networkx as nx
from neo4j import GraphDatabase

class Neo4jShardLoader:
    def __init__(self, partition_assignment, graph):
        self.partition = partition_assignment
        self.graph = graph
        
        # Shard connection URIs
        self.shard_uris = {
            0: "bolt://localhost:7687",
            1: "bolt://localhost:7688",
            2: "bolt://localhost:7689"
        }
        
        self.auth = ("neo4j", "password123")
    
    def clear_shard(self, shard_id):
        """Clear all data from a shard"""
        driver = GraphDatabase.driver(self.shard_uris[shard_id], auth=self.auth)
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        driver.close()
        print(f"Cleared shard {shard_id}")
    
    def load_partition_to_shard(self, shard_id):
        """Load a partition into its shard"""
        driver = GraphDatabase.driver(self.shard_uris[shard_id], auth=self.auth)
        
        # Get nodes in this partition
        partition_nodes = [node for node, part in self.partition.items() 
                          if part == shard_id]
        
        print(f"\nLoading {len(partition_nodes)} nodes to shard {shard_id}...")
        
        with driver.session() as session:
            # Create nodes
            for node_id in partition_nodes:
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
            
            # Create relationships (within shard only)
            edge_count = 0
            proxy_needed = set()
            
            for u, v in self.graph.edges():
                part_u = self.partition[u]
                part_v = self.partition[v]
                
                # Both nodes in this shard - create actual relationship
                if part_u == shard_id and part_v == shard_id:
                    session.run("""
                        MATCH (u:Person {id: $u_id})
                        MATCH (v:Person {id: $v_id})
                        CREATE (u)-[:KNOWS]->(v)
                    """, u_id=u, v_id=v)
                    edge_count += 1
                
                # Need proxy: u is here, v is in another shard
                elif part_u == shard_id and part_v != shard_id:
                    proxy_needed.add(v)
                
                # Need proxy: v is here, u is in another shard  
                elif part_v == shard_id and part_u != shard_id:
                    proxy_needed.add(u)
            
            print(f"Created {edge_count} internal relationships")
            
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
                
                # u in this shard, v needs proxy
                if part_u == shard_id and part_v != shard_id:
                    session.run("""
                        MATCH (u:Person {id: $u_id})
                        MATCH (p:PersonProxy {id: $proxy_id})
                        CREATE (u)-[:KNOWS]->(p)
                    """, u_id=u, proxy_id=v)
                    proxy_edge_count += 1
                
                # v in this shard, u needs proxy
                elif part_v == shard_id and part_u != shard_id:
                    session.run("""
                        MATCH (v:Person {id: $v_id})
                        MATCH (p:PersonProxy {id: $proxy_id})
                        CREATE (v)-[:KNOWS]->(p)
                    """, v_id=v, proxy_id=u)
                    proxy_edge_count += 1
            
            print(f"Created {len(proxy_needed)} proxy nodes")
            print(f"Created {proxy_edge_count} proxy relationships")
        
        driver.close()

# Load data
with open('../data/graph.pkl', 'rb') as f:
    graph = pickle.load(f)['graph']

with open('../data/refined_partition.pkl', 'rb') as f:
    partition = pickle.load(f)['partition_assignment']

# Load to Neo4j
loader = Neo4jShardLoader(partition, graph)

for shard_id in range(3):
    loader.clear_shard(shard_id)
    loader.load_partition_to_shard(shard_id)

print("\nAll shards loaded!")