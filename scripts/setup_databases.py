from neo4j import GraphDatabase
import time


class DatabaseSetup:
    def __init__(self, uri="bolt://localhost:7687", auth=("neo4j", "password123")):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def create_databases(self):
        """Create three shard databases"""
        print("Creating shard databases...")

        with self.driver.session(database="system") as session:
            # Create shard databases
            for shard_id in [1, 2, 3]:
                db_name = f"shard{shard_id}"

                # Drop if exists
                try:
                    session.run(f"DROP DATABASE {db_name} IF EXISTS")
                    time.sleep(2)
                except Exception as e:
                    print(f"Note: {e}")

                # Create database
                session.run(f"CREATE DATABASE {db_name}")
                print(f"Created database: {db_name}")
                time.sleep(1)

        print("\nAll shard databases created!")
        print("Available databases: shard1, shard2, shard3")

    def verify_databases(self):
        """List all databases"""
        with self.driver.session(database="system") as session:
            result = session.run("SHOW DATABASES")
            print("\n=== Available Databases ===")
            for record in result:
                print(
                    f"  - {record['name']} (status: {record['currentStatus']})")

    def close(self):
        self.driver.close()


# Run setup
setup = DatabaseSetup()
setup.create_databases()
setup.verify_databases()
setup.close()

print("\n✓ Database setup complete!")
