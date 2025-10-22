import networkx as nx
import numpy as np
import pickle

class MultiTenantGraphGenerator:
    def __init__(self, num_tenants=100, seed=42):
        self.num_tenants = num_tenants
        self.seed = seed
        np.random.seed(seed)
        
    def generate_tenant_sizes_zipf(self, alpha=1.5):
        """Generate tenant sizes following Zipf distribution"""
        ranks = np.arange(1, self.num_tenants + 1)
        sizes = (1000 * (ranks ** -alpha)).astype(int)
        # Ensure minimum size of 50 nodes
        sizes = np.maximum(sizes, 50)
        return sizes
    
    def generate_social_network(self, num_nodes):
        """Generate a social network for a tenant"""
        # Barabasi-Albert preferential attachment
        m = 3  # Number of edges to attach from new node
        G = nx.barabasi_albert_graph(num_nodes, m, seed=self.seed)
        
        # Add node attributes
        for node_id in G.nodes():
            G.nodes[node_id]['name'] = f"User_{node_id}"
            G.nodes[node_id]['age'] = np.random.randint(18, 70)
            G.nodes[node_id]['region'] = np.random.choice(['US', 'EU', 'ASIA'])
        
        return G
    
    def generate_multi_tenant_graph(self):
        """Generate complete multi-tenant graph"""
        tenant_sizes = self.generate_tenant_sizes_zipf()
        
        combined_graph = nx.Graph()
        tenant_info = []
        node_offset = 0
        
        for tenant_id, size in enumerate(tenant_sizes):
            print(f"Generating tenant {tenant_id+1}/{self.num_tenants}, size={size}")
            
            # Generate tenant subgraph
            tenant_graph = self.generate_social_network(size)
            
            # Relabel nodes with global IDs
            mapping = {old_id: old_id + node_offset 
                      for old_id in tenant_graph.nodes()}
            tenant_graph = nx.relabel_nodes(tenant_graph, mapping)
            
            # Add tenant_id to all nodes
            for node_id in tenant_graph.nodes():
                tenant_graph.nodes[node_id]['tenant_id'] = tenant_id
            
            # Merge into combined graph
            combined_graph = nx.compose(combined_graph, tenant_graph)
            
            tenant_info.append({
                'tenant_id': tenant_id,
                'size': size,
                'node_range': (node_offset, node_offset + size)
            })
            
            node_offset += size
        
        print(f"\nTotal graph: {combined_graph.number_of_nodes()} nodes, "
              f"{combined_graph.number_of_edges()} edges")
        
        return combined_graph, tenant_info

# Generate and save
generator = MultiTenantGraphGenerator(num_tenants=100)
graph, tenant_info = generator.generate_multi_tenant_graph()

# Save to file
with open('../data/graph.pkl', 'wb') as f:
    pickle.dump({'graph': graph, 'tenant_info': tenant_info}, f)

print("Graph saved to data/graph.pkl")