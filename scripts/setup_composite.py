from neo4j import GraphDatabase
import time

class CompositeSetup:
    def __init__(self, uri="bolt://localhost:7687", auth=("neo4j", "password123")):
        self.driver = GraphDatabase.driver(uri, auth=auth)
    
    def create_composite(self):
        """Create composite database from local databases (idempotent)"""
        print("Setting up composite database...")
        with self.driver.session(database="system") as session:
            # Drop existing aliases first (ignore errors)
            aliases_to_drop = ['mycomposite.shard1', 'mycomposite.shard2', 'mycomposite.shard3']
            for alias in aliases_to_drop:
                try:
                    session.run(f"DROP ALIAS {alias} FOR DATABASE")
                    print(f"Dropped alias: {alias}")
                except Exception as e:
                    print(f"Note: {e}")
                time.sleep(0.5)  # Short pause
                
            # Drop composite database without "FOR DATABASE"
            try:
                session.run("DROP COMPOSITE DATABASE mycomposite")
                session.run("DROP DATABASE mycomposite")
                print("Dropped existing composite: mycomposite")
                time.sleep(2)
            except Exception as e:
                print(f"Note: {e}")
            
            # Create composite database
            try:
                session.run("CREATE COMPOSITE DATABASE mycomposite")
                print("Created composite database: mycomposite")
                time.sleep(2)
            except Exception as e:
                if 'already exists' in str(e):
                    print("Composite database mycomposite already exists, skipping creation.")
                else:
                    print(f"Error creating composite: {e}")
                    raise
            
            # Create/replace aliases (idempotent)
            session.run("CREATE OR REPLACE ALIAS mycomposite.shard1 FOR DATABASE shard1")
            session.run("CREATE OR REPLACE ALIAS mycomposite.shard2 FOR DATABASE shard2")
            session.run("CREATE OR REPLACE ALIAS mycomposite.shard3 FOR DATABASE shard3")
            print("Added constituent databases:")
            print(" - shard1 (via alias mycomposite.shard1)")
            print(" - shard2 (via alias mycomposite.shard2)")
            print(" - shard3 (via alias mycomposite.shard3)")
    
    def verify_composite(self):
        """Verify composite database setup"""
        with self.driver.session(database="system") as session:
            result = session.run("SHOW DATABASES")
            print("\n=== All Databases ===")
            for record in result:
                db_type = record['type'] or 'standard'
                print(f" {record['name']}: {db_type} ({record['currentStatus']})")
            
            result = session.run("SHOW ALIASES FOR DATABASE")
            print("\n=== Composite Constituents ===")
            has_results = False
            for record in result:
                alias_name = record['name']
                target_db = record['database'] or record['target']
                if alias_name.startswith('mycomposite.'):
                    print(f" {alias_name} -> {target_db}")
                    has_results = True
            if not has_results:
                print("No aliases found for mycomposite (check manually with SHOW ALIAS)")
    
    def test_composite_query(self):
        """Test a simple composite query"""
        print("\n=== Testing Composite Query ===")
        with self.driver.session(database="mycomposite") as session:
            try:
                result = session.run("""
                    CALL {
                        USE mycomposite.shard1
                        MATCH (n:Person) RETURN count(n) as count
                        UNION
                        USE mycomposite.shard2
                        MATCH (n:Person) RETURN count(n) as count
                        UNION
                        USE mycomposite.shard3
                        MATCH (n:Person) RETURN count(n) as count
                    }
                    RETURN sum(count) as total_persons
                """)
                for record in result:
                    print(f"Total persons across all shards: {record['total_persons']}")
            except Exception as e:
                print(f"Query error (expected if no data loaded): {e}")
            try:
                result = session.run("""
                    CALL {
                        USE mycomposite.shard1
                        MATCH (n:PersonProxy) RETURN count(n) as count
                        UNION
                        USE mycomposite.shard2
                        MATCH (n:PersonProxy) RETURN count(n) as count
                        UNION
                        USE mycomposite.shard3
                        MATCH (n:PersonProxy) RETURN count(n) as count
                    }
                    RETURN sum(count) as total_proxies
                """)
                for record in result:
                    print(f"Total proxy nodes: {record['total_proxies']}")
            except Exception as e:
                print(f"Proxy query error (expected if no data): {e}")
    
    def close(self):
        self.driver.close()


# Setup composite
if __name__ == "__main__":
    setup = CompositeSetup()
    setup.create_composite()
    setup.verify_composite()
    setup.test_composite_query()
    setup.close()

    print("\n✓ Composite database setup complete!")
