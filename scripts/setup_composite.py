from neo4j import GraphDatabase

# Connect to shard1 (we'll create composite here)
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password123"))

with driver.session(database="system") as session:
    # Create aliases for other shards
    session.run("""
        CREATE ALIAS shard2 
        FOR DATABASE neo4j 
        AT 'neo4j://neo4j-shard2:7688' 
        USER neo4j 
        PASSWORD 'password123'
    """)
    
    session.run("""
        CREATE ALIAS shard3 
        FOR DATABASE neo4j 
        AT 'neo4j://neo4j-shard3:7689' 
        USER neo4j 
        PASSWORD 'password123'
    """)
    
    # Create composite database
    session.run("CREATE COMPOSITE DATABASE mycomposite")
    
    # Add constituents
    session.run("ALTER DATABASE mycomposite ADD COMPOSITE neo4j")  # shard1
    session.run("ALTER DATABASE mycomposite ADD COMPOSITE shard2")
    session.run("ALTER DATABASE mycomposite ADD COMPOSITE shard3")

print("Composite database 'mycomposite' created!")
driver.close()