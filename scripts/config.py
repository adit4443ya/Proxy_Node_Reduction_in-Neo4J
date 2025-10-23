"""
Configuration for Dynamic Proxy Management System

This module contains all tunable parameters for the adaptive proxy node
management system. Adjust these values to control the behavior of the
dynamic optimization.
"""

# ============================================================================
# QUERY MONITORING CONFIGURATION
# ============================================================================

# Time window for calculating statistics (seconds)
STATS_TIME_WINDOW = 300  # 5 minutes

# Decay factor for exponential moving average (0-1)
# Higher = more weight on recent queries
QUERY_DECAY_FACTOR = 0.7

# Minimum queries before considering a proxy for promotion
MIN_QUERIES_THRESHOLD = 10

# ============================================================================
# SCORING CONFIGURATION
# ============================================================================

# Weight components for scoring formula
FREQUENCY_WEIGHT = 1.0       # How much to weight access frequency
CROSS_SHARD_WEIGHT = 2.0     # Penalty for cross-shard accesses
LATENCY_WEIGHT = 1.5         # Weight for query latency
COLOCATION_BONUS = 0.5       # Bonus for co-located neighbors

# Baseline latency for normalization (ms)
BASELINE_LATENCY = 10.0

# Cost per node replication (abstract units)
REPLICATION_COST_BASE = 5.0
REPLICATION_COST_PER_EDGE = 0.1

# ============================================================================
# DYNAMIC MANAGEMENT CONFIGURATION
# ============================================================================

# Strategy selection: 'greedy', 'threshold', 'budget_aware'
MANAGEMENT_STRATEGY = 'budget_aware'

# Evaluation frequency (queries between evaluations)
EVALUATION_FREQUENCY = 100

# Greedy strategy: Top K proxies to promote per shard
GREEDY_TOP_K = 10

# Threshold strategy: Score thresholds
PROMOTION_THRESHOLD = 15.0
DEMOTION_THRESHOLD = 2.0

# Budget strategy: Max percentage of nodes that can be replications
MAX_REPLICATION_PERCENTAGE = 0.15  # 15% of nodes per shard
MIN_REPLICATION_PERCENTAGE = 0.05  # 5% minimum to keep system dynamic

# ============================================================================
# ADAPTIVE BENCHMARK CONFIGURATION
# ============================================================================

# Number of queries to run in adaptive benchmark
ADAPTIVE_BENCHMARK_QUERIES = 1000

# Reporting frequency (queries between progress reports)
REPORT_FREQUENCY = 50

# Enable/disable different optimization features
ENABLE_PROMOTION = True      # Convert PersonProxy -> Person
ENABLE_REPLICATION = True    # Copy Person to multiple shards
ENABLE_DEMOTION = False      # Convert Person -> PersonProxy (risky!)

# ============================================================================
# NEO4J CONNECTION
# ============================================================================

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"

SHARD_DATABASES = ["shard1", "shard2", "shard3"]
COMPOSITE_DATABASE = "mycomposite"

# ============================================================================
# LOGGING AND DEBUGGING
# ============================================================================

# Enable verbose logging
VERBOSE = True

# Save intermediate results
SAVE_MONITORING_DATA = True
MONITORING_DATA_PATH = "../data/monitoring_stats.pkl"

# Save optimization history
SAVE_OPTIMIZATION_HISTORY = True
OPTIMIZATION_HISTORY_PATH = "../data/optimization_history.pkl"

# ============================================================================
# ADVANCED TUNING
# ============================================================================

# Query pattern detection
DETECT_QUERY_PATTERNS = True  # Detect temporal patterns (e.g., daily cycles)
PATTERN_DETECTION_WINDOW = 1000  # Queries to analyze for patterns

# Hot/cold node classification
HOT_NODE_PERCENTILE = 90  # Nodes above this percentile are "hot"
COLD_NODE_PERCENTILE = 10  # Nodes below this are "cold"

# Batch size for Neo4j operations
BATCH_SIZE = 1000

# Cache settings (for frequently accessed metadata)
ENABLE_METADATA_CACHE = True
CACHE_TTL = 60  # seconds

# ============================================================================
# EXPERIMENTAL FEATURES
# ============================================================================

# Predictive replication based on graph structure
ENABLE_PREDICTIVE_REPLICATION = False
PREDICTION_LOOKAHEAD = 2  # Hops to look ahead

# Collaborative filtering for proxy placement
ENABLE_COLLABORATIVE_FILTERING = False

# Machine learning-based scoring (requires training)
ENABLE_ML_SCORING = False
ML_MODEL_PATH = "../models/proxy_scorer.pkl"
