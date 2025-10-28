from neo4j import GraphDatabase
import time


class DatabaseSetup:
    def __init__(self, uri="bolt://localhost:7687", auth=("neo4j", "password123")):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def create_databases(self):
        """Create three shard databases"""

        with self.driver.session(database="system") as session:
            # Drop aliases pointing to shards before dropping databases
            aliases = ['mycomposite.shard1', 'mycomposite.shard2', 'mycomposite.shard3']
            for alias in aliases:
                try:
                    session.run(f"DROP ALIAS {alias} FOR DATABASE")
                    print(f"Dropped alias: {alias}")
                except Exception as e:
                    print(f"Alias {alias} not found or already dropped: {e}")

            # Drop and recreate shard databases
            for shard_id in [1, 2, 3]:
                db_name = f"shard{shard_id}"

                try:
                    session.run(f"DROP DATABASE {db_name} IF EXISTS")
                    print(f"Dropped database {db_name} (if it existed)")
                    time.sleep(2)
                except Exception as e:
                    print(f"Note: {e}")

                session.run(f"CREATE DATABASE {db_name}")
                print(f"Created database: {db_name}")
                time.sleep(1)

                try:
                    session.run(f"START DATABASE {db_name}")
                    print(f"Started database: {db_name}")
                    time.sleep(5)  # Wait for DB to come online
                except Exception as e:
                    print(f"Could not start {db_name}: {e}")

        print("\nAll shard databases created!")
        print("Available databases: shard1, shard2, shard3")

    def verify_databases(self):
        """List all databases"""
        with self.driver.session(database="system") as session:
            result = session.run("SHOW DATABASES")
            print("\n=== Available Databases ===")
            for record in result:
                print(f"  - {record['name']} (status: {record['currentStatus']})")

    def close(self):
        self.driver.close()


# Run setup
if __name__ == "__main__":
    setup = DatabaseSetup()
    setup.create_databases()
    setup.verify_databases()
    setup.close()

    print("\n✓ Database setup complete!")
