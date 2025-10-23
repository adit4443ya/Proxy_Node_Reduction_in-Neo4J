# Dynamic Proxy Node Management System

## Overview

This system implements **adaptive proxy node management** for distributed graph databases. It dynamically monitors query patterns and intelligently replicates or promotes proxy nodes to reduce cross-shard query latency.

### Key Innovation

Traditional static partitioning creates proxy nodes at load time and never adjusts them. This dynamic system:

1. **Monitors** query access patterns in real-time
2. **Scores** proxy nodes based on access frequency, cross-shard cost, and latency impact
3. **Optimizes** proxy placement by promoting frequently-accessed proxies to full nodes
4. **Adapts** continuously as query patterns change

### Performance Benefits

- **Reduced Latency**: Frequently-accessed remote nodes become local
- **Lower Cross-Shard Queries**: Hot proxies promoted to full nodes
- **Adaptive**: System learns from query patterns automatically
- **Configurable**: Multiple strategies and tunable parameters

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Adaptive Query System                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐      ┌──────────────┐      ┌───────────┐ │
│  │   Query      │─────>│   Proxy      │─────>│  Dynamic  │ │
│  │  Monitor     │      │   Scorer     │      │  Manager  │ │
│  └──────────────┘      └──────────────┘      └───────────┘ │
│         │                      │                     │       │
│    Access Stats            Scores                Actions     │
│         ▼                      ▼                     ▼       │
│  ┌─────────────────────────────────────────────────────────┐│
│  │           Neo4j Shard Databases                          ││
│  │   shard1        shard2         shard3                    ││
│  │  Person         Person         Person                    ││
│  │  PersonProxy    PersonProxy    PersonProxy               ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

### Components

#### 1. Query Monitor (`query_monitor.py`)
Tracks query access patterns across shards.

**Tracked Metrics:**
- Access frequency per node
- Cross-shard vs local access ratio
- Query latency per node
- Temporal access patterns

**Key Classes:**
- `ProxyAccessStats`: Statistics for individual node accesses
- `QueryMonitor`: Main monitoring coordinator

#### 2. Proxy Scorer (`proxy_scorer.py`)
Scores proxy nodes to determine replication priorities.

**Scoring Formula:**
```
score = (frequency × cross_shard_ratio × latency_impact) + colocation_bonus - replication_cost

where:
  frequency         = queries per minute
  cross_shard_ratio = remote_accesses / total_accesses
  latency_impact    = avg_latency / baseline_latency
  colocation_bonus  = neighbors_in_target_shard / total_neighbors
  replication_cost  = base_cost + (degree × cost_per_edge)
```

**Key Classes:**
- `ProxyScore`: Score with detailed component breakdown
- `ProxyScorer`: Scoring engine

#### 3. Dynamic Proxy Manager (`dynamic_proxy_manager.py`)
Executes optimization decisions on Neo4j databases.

**Actions:**
- **Promote**: Convert `PersonProxy` → `Person` (full node data)
- **Replicate**: Copy `Person` to additional shards
- **Demote**: Convert `Person` → `PersonProxy` (if underused)

**Strategies:**
- `greedy`: Top-K scoring proxies per evaluation
- `threshold`: Score-based promotion triggers
- `budget_aware`: Constrained optimization with capacity limits

**Key Classes:**
- `ReplicationAction`: Metadata for optimization actions
- `DynamicProxyManager`: Main orchestrator

#### 4. Adaptive Benchmark (`adaptive_benchmark.py`)
Runs queries with live monitoring and optimization.

**Features:**
- Executes queries while monitoring access patterns
- Periodic optimization evaluations
- Latency trend analysis
- Performance comparison metrics

---

## Configuration

All tunable parameters are in `config.py`:

### Monitoring Parameters

```python
STATS_TIME_WINDOW = 300         # Statistics window (seconds)
QUERY_DECAY_FACTOR = 0.7        # Weight on recent queries (0-1)
MIN_QUERIES_THRESHOLD = 10      # Min queries before considering proxy
```

### Scoring Weights

```python
FREQUENCY_WEIGHT = 1.0          # Access frequency weight
CROSS_SHARD_WEIGHT = 2.0        # Cross-shard penalty weight
LATENCY_WEIGHT = 1.5            # Latency impact weight
COLOCATION_BONUS = 0.5          # Co-location benefit bonus

BASELINE_LATENCY = 10.0         # Baseline latency for normalization (ms)
REPLICATION_COST_BASE = 5.0     # Base cost per node replication
REPLICATION_COST_PER_EDGE = 0.1 # Cost per edge in replication
```

### Management Strategy

```python
MANAGEMENT_STRATEGY = 'budget_aware'  # 'greedy', 'threshold', 'budget_aware'
EVALUATION_FREQUENCY = 100            # Queries between evaluations

# Greedy strategy
GREEDY_TOP_K = 10                     # Top K proxies per shard

# Threshold strategy
PROMOTION_THRESHOLD = 15.0            # Score threshold for promotion
DEMOTION_THRESHOLD = 2.0              # Score threshold for demotion

# Budget strategy
MAX_REPLICATION_PERCENTAGE = 0.15     # Max 15% replicated nodes per shard
```

### Optimization Features

```python
ENABLE_PROMOTION = True         # Convert PersonProxy -> Person
ENABLE_REPLICATION = True       # Copy Person to multiple shards
ENABLE_DEMOTION = False         # Convert Person -> PersonProxy (risky!)
```

---

## Usage Guide

### Basic Workflow

```bash
# 1. Standard setup (as before)
cd docker && docker-compose up -d
cd ../scripts
python setup_databases.py
python generate_graph.py
python partition_graph.py
python proxy_refinement.py
python load_to_neo4j.py
python setup_composite.py

# 2. Run STANDARD (static) benchmark for baseline
python benchmark_queries.py

# 3. Run ADAPTIVE (dynamic) benchmark
python adaptive_benchmark.py

# 4. Compare results
python compare_benchmarks.py
```

### Running Adaptive Benchmark

```python
from adaptive_benchmark import AdaptiveBenchmark

# Create benchmark
benchmark = AdaptiveBenchmark()

# Run with monitoring and optimization
benchmark.run_benchmark()

# Analyze results
benchmark.analyze_results()

# Save results
benchmark.save_results()

# Cleanup
benchmark.cleanup()
```

### Customizing Strategy

Edit `config.py` before running:

```python
# For aggressive optimization
MANAGEMENT_STRATEGY = 'greedy'
GREEDY_TOP_K = 20
EVALUATION_FREQUENCY = 50

# For conservative optimization
MANAGEMENT_STRATEGY = 'threshold'
PROMOTION_THRESHOLD = 25.0
EVALUATION_FREQUENCY = 200

# For budget-constrained optimization
MANAGEMENT_STRATEGY = 'budget_aware'
MAX_REPLICATION_PERCENTAGE = 0.10  # Max 10%
EVALUATION_FREQUENCY = 100
```

---

## Scoring Algorithm Details

### Component Breakdown

#### 1. Frequency Component
Measures how often a proxy is accessed from the target shard.

```python
frequency = (accesses_from_shard / time_span) * 60  # per minute
frequency_component = frequency * FREQUENCY_WEIGHT
```

**Intuition**: More frequent accesses = higher replication benefit.

#### 2. Cross-Shard Component
Penalizes proxies with high cross-shard access ratios.

```python
cross_shard_ratio = cross_shard_accesses / total_accesses
cross_shard_component = cross_shard_ratio * CROSS_SHARD_WEIGHT
```

**Intuition**: High cross-shard ratio = expensive remote accesses = high replication value.

#### 3. Latency Component
Weights by query latency impact.

```python
latency_normalized = avg_latency / BASELINE_LATENCY
latency_component = latency_normalized * LATENCY_WEIGHT
```

**Intuition**: High latency queries benefit more from replication.

#### 4. Co-location Bonus
Bonus for replicating near neighbors.

```python
colocation_ratio = neighbors_in_target_shard / total_neighbors
colocation_component = colocation_ratio * COLOCATION_BONUS
```

**Intuition**: Replicating near neighbors improves traversal locality.

#### 5. Replication Cost
Cost of storing and maintaining the replica.

```python
replication_cost = REPLICATION_COST_BASE + (degree * REPLICATION_COST_PER_EDGE)
```

**Intuition**: High-degree nodes are more expensive to replicate.

### Final Score Calculation

```python
score = (frequency_component * cross_shard_component * latency_component
         + colocation_component - replication_cost)
```

**Decision Rule:**
- `score > 0`: Replication is beneficial
- `score > PROMOTION_THRESHOLD`: Immediate promotion (threshold strategy)
- Top-K scores: Selected for promotion (greedy strategy)
- Within budget: Promoted (budget-aware strategy)

---

## Management Strategies

### 1. Greedy Strategy

**Algorithm:**
```
Every N queries:
  For each shard:
    Score all proxies for that shard
    Sort by score (descending)
    Promote top K proxies
```

**Pros:**
- Simple and predictable
- Consistent optimization pace

**Cons:**
- May promote low-scoring proxies if K is too high
- Fixed promotion rate regardless of benefit

**Best For:**
- Stable query patterns
- Predictable workloads

### 2. Threshold Strategy

**Algorithm:**
```
Every N queries:
  For each shard:
    Score all proxies
    Promote proxies with score > threshold
    Demote replicas with score < demotion_threshold (if enabled)
```

**Pros:**
- Adaptive to query patterns
- Only promotes high-value proxies
- Can demote underused replications

**Cons:**
- Variable promotion rate (can be zero or many)
- May exceed budget if many proxies score high

**Best For:**
- Variable query patterns
- Workloads with clear hot/cold nodes

### 3. Budget-Aware Strategy (Recommended)

**Algorithm:**
```
Every N queries:
  For each shard:
    Calculate available budget (max_replications - current_replications)
    Score all proxies
    Promote top N proxies within budget
    Evict lowest-scored replicas if at capacity
```

**Pros:**
- Bounded resource usage
- Prioritizes highest-value replications
- Automatic eviction when at capacity

**Cons:**
- More complex logic
- May not promote all beneficial proxies if budget is tight

**Best For:**
- Production environments with resource constraints
- Multi-tenant systems
- Long-running workloads

---

## Monitoring and Analysis

### Real-Time Monitoring

The `QueryMonitor` tracks:

```python
monitor.get_statistics()
# Returns:
{
    'total_queries': 1000,
    'cross_shard_queries': 450,
    'cross_shard_percentage': 45.0,
    'avg_latency_ms': 12.5,
    'hot_nodes_count': 25,
    'unique_nodes_accessed': 300
}
```

### Scoring Analysis

```python
scorer.analyze_score_distribution()
# Returns:
{
    'total_candidates': 150,
    'min_score': 2.3,
    'max_score': 45.8,
    'median_score': 12.1,
    'p95_score': 35.2,
    'above_threshold': 23
}
```

### Optimization History

```python
manager.get_statistics()
# Returns:
{
    'evaluation_count': 10,
    'promotions_executed': 87,
    'unique_nodes_replicated': 52,
    'replications_per_shard': {0: 28, 1: 31, 2: 28}
}
```

---

## Performance Optimization Tips

### 1. Tuning Evaluation Frequency

```python
# More frequent evaluations (responsive but higher overhead)
EVALUATION_FREQUENCY = 50

# Less frequent evaluations (lower overhead but slower adaptation)
EVALUATION_FREQUENCY = 200

# Recommended for most workloads
EVALUATION_FREQUENCY = 100
```

### 2. Adjusting Replication Budget

```python
# Aggressive replication (more storage, lower latency)
MAX_REPLICATION_PERCENTAGE = 0.25  # 25%

# Conservative replication (less storage, higher latency)
MAX_REPLICATION_PERCENTAGE = 0.10  # 10%

# Balanced (recommended)
MAX_REPLICATION_PERCENTAGE = 0.15  # 15%
```

### 3. Tuning Score Weights

For latency-sensitive workloads:
```python
LATENCY_WEIGHT = 2.0      # Increase latency weight
FREQUENCY_WEIGHT = 1.0
CROSS_SHARD_WEIGHT = 1.5
```

For throughput-focused workloads:
```python
FREQUENCY_WEIGHT = 2.0    # Prioritize high-frequency accesses
LATENCY_WEIGHT = 1.0
CROSS_SHARD_WEIGHT = 2.0
```

For storage-constrained environments:
```python
REPLICATION_COST_BASE = 10.0      # Increase replication cost
REPLICATION_COST_PER_EDGE = 0.5   # Penalize high-degree nodes
```

### 4. Query Pattern Detection

Enable temporal pattern detection:
```python
DETECT_QUERY_PATTERNS = True
PATTERN_DETECTION_WINDOW = 1000
```

This identifies:
- **Steady**: Consistent access patterns
- **Moderate**: Variable access patterns
- **Bursty**: Sporadic spike patterns

Use this information to adjust strategy selection.

---

## Troubleshooting

### Issue: No proxies are promoted

**Possible Causes:**
1. Scores below threshold (threshold strategy)
2. Budget already exhausted (budget-aware strategy)
3. Insufficient queries (< MIN_QUERIES_THRESHOLD)

**Solutions:**
```python
# Lower threshold
PROMOTION_THRESHOLD = 10.0

# Increase budget
MAX_REPLICATION_PERCENTAGE = 0.20

# Lower minimum queries
MIN_QUERIES_THRESHOLD = 5
```

### Issue: Too many replications

**Possible Causes:**
1. Budget too high
2. Greedy K too large
3. Threshold too low

**Solutions:**
```python
# Reduce budget
MAX_REPLICATION_PERCENTAGE = 0.10

# Use smaller K
GREEDY_TOP_K = 5

# Raise threshold
PROMOTION_THRESHOLD = 20.0
```

### Issue: Performance not improving

**Possible Causes:**
1. Wrong nodes being promoted
2. Query pattern doesn't match optimization
3. Evaluation frequency too low

**Solutions:**
1. Analyze score components:
   ```python
   scorer.print_top_scores(k=10)
   ```
2. Check query patterns:
   ```python
   monitor.detect_access_patterns()
   ```
3. Increase evaluation frequency:
   ```python
   EVALUATION_FREQUENCY = 50
   ```

---

## Advanced Features

### Predictive Replication (Experimental)

```python
ENABLE_PREDICTIVE_REPLICATION = True
PREDICTION_LOOKAHEAD = 2  # Hops ahead to predict accesses
```

Proactively replicates nodes based on graph structure.

### Collaborative Filtering (Experimental)

```python
ENABLE_COLLABORATIVE_FILTERING = True
```

Uses query similarity to predict which proxies will be accessed together.

### Machine Learning Scoring (Experimental)

```python
ENABLE_ML_SCORING = True
ML_MODEL_PATH = "../models/proxy_scorer.pkl"
```

Train a model on historical data to predict replication benefit.

---

## Benchmark Comparison

### Standard Benchmark
- **Approach**: Static partitioning with fixed proxies
- **Latency**: Baseline performance
- **File**: `benchmark_queries.py`

### Adaptive Benchmark
- **Approach**: Dynamic proxy management with live optimization
- **Latency**: Potentially reduced through adaptation
- **File**: `adaptive_benchmark.py`

### Comparison Tool
```bash
python compare_benchmarks.py
```

Generates:
- Side-by-side statistics
- Improvement percentages
- Latency trend visualization
- Summary report

---

## Data Persistence

### Monitoring Data
```python
# Save
monitor.save_to_file("../data/monitoring_stats.pkl")

# Load
monitor.load_from_file("../data/monitoring_stats.pkl")
```

### Optimization History
```python
# Save
manager.save_history("../data/optimization_history.pkl")
```

### Benchmark Results
```python
# Save
benchmark.save_results("../results/adaptive_benchmark_results.pkl")
```

---

## Future Enhancements

1. **Multi-Objective Optimization**
   - Balance latency, storage, and consistency costs
   - Pareto-optimal replication sets

2. **Temporal Adaptation**
   - Detect daily/weekly query patterns
   - Proactive replication before peak times

3. **Federated Learning**
   - Learn from multiple deployments
   - Transfer optimization strategies

4. **Auto-Tuning**
   - Automatically adjust weights and thresholds
   - Reinforcement learning-based parameter selection

5. **Cost-Based Optimization**
   - Incorporate actual storage costs
   - Network bandwidth constraints
   - Query SLA requirements

---

## References

### Related Work
- Graph partitioning: METIS algorithm
- Proxy node patterns in distributed databases
- Adaptive caching strategies
- Query workload analysis

### Neo4j Features Used
- Composite databases (5.15+)
- Multi-database architecture
- Cypher query federation
- Bolt protocol

---

## License

Same as parent project.

## Contributors

Dynamic proxy management system designed and implemented for adaptive graph query optimization in Neo4j distributed environments.
