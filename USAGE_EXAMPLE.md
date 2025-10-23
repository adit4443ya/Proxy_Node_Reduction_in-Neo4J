# Dynamic Proxy Management - Usage Examples

This document provides practical examples of using the dynamic proxy management system.

---

## Table of Contents

1. [Basic Setup](#basic-setup)
2. [Running Standard Benchmark](#running-standard-benchmark)
3. [Running Adaptive Benchmark](#running-adaptive-benchmark)
4. [Comparing Results](#comparing-results)
5. [Custom Configuration](#custom-configuration)
6. [Advanced Usage](#advanced-usage)

---

## Basic Setup

### 1. Install Dependencies

```bash
# Install Python packages
pip install -r requirements.txt

# Verify installation
pip list | grep -E "networkx|neo4j|numpy|pymetis"
```

### 2. Start Neo4j

```bash
# Start Neo4j container
cd docker
docker-compose up -d

# Verify Neo4j is running
docker ps | grep neo4j

# Check logs
docker logs neo4j-enterprise
```

### 3. Initialize Databases

```bash
cd ../scripts

# Create databases
python setup_databases.py

# Expected output:
# ✓ Created database: shard1
# ✓ Created database: shard2
# ✓ Created database: shard3
```

### 4. Generate and Partition Graph

```bash
# Generate multi-tenant graph
python generate_graph.py
# Output: data/graph.pkl

# Partition with METIS
python partition_graph.py
# Output: data/metis_partition.pkl

# Refine partitioning
python proxy_refinement.py
# Output: data/refined_partition.pkl
```

### 5. Load Data to Neo4j

```bash
# Load partitions to shards
python load_to_neo4j.py

# Create composite database
python setup_composite.py
```

---

## Running Standard Benchmark

The standard benchmark uses static proxy placement:

```bash
python benchmark_queries.py
```

**Example Output:**
```
======================================================================
BENCHMARK RESULTS
======================================================================
Queries executed: 100
Total time: 2.45 seconds

Latency Statistics:
  Mean:     24.50 ms
  Median:   22.00 ms
  P95:      38.20 ms
  P99:      45.60 ms

Proxy Analysis:
  Shard 1: 12.3% proxy nodes
  Shard 2: 11.8% proxy nodes
  Shard 3: 12.5% proxy nodes

Results saved to: ../results/benchmark_results.pkl
======================================================================
```

**What It Measures:**
- Query latency with static proxy placement
- Baseline performance metrics
- Proxy node overhead per shard

---

## Running Adaptive Benchmark

The adaptive benchmark enables dynamic proxy management:

```bash
python adaptive_benchmark.py
```

**Example Output:**
```
======================================================================
STARTING ADAPTIVE BENCHMARK
======================================================================
Total Queries: 1000
Strategy: budget_aware
Optimization Frequency: every 100 queries
======================================================================

[Query 100/1000] Window Avg Latency: 24.30ms

======================================================================
OPTIMIZATION EVALUATION #1
Query count: 100
======================================================================

======================================================================
TOP PROXY REPLICATION CANDIDATES
======================================================================

[Shard 0] Top 5 candidates:
  1. Node 42 (from shard 1)
     Score: 25.34 | Accesses: 18 | Latency: 28.5ms
     Components: freq=3.60, cross=1.80, latency=1.71, coloc=0.33, cost=5.20

  2. Node 87 (from shard 2)
     Score: 18.92 | Accesses: 14 | Latency: 22.1ms
     ...

✓ Executed 7 optimization actions
  - ReplicationAction(promote: node 42 from shard 1 to shard 0, score=25.34)
  - ReplicationAction(promote: node 87 from shard 2 to shard 0, score=18.92)
  ...

[Progress] 200/1000 queries | 81.6 q/s | avg latency: 21.30ms

...

======================================================================
BENCHMARK COMPLETED
======================================================================
Total Time: 12.25 seconds
Queries Executed: 1000
Avg QPS: 81.63

======================================================================
BENCHMARK ANALYSIS
======================================================================

Latency Statistics:
  Mean:   19.30 ms
  Median: 17.50 ms
  Min:    8.20 ms
  Max:    42.10 ms
  P95:    28.90 ms
  P99:    35.20 ms

Latency Trend (by window):
  Query 100: 24.30ms
  Query 200: 21.50ms
  Query 300: 19.80ms
  Query 400: 18.90ms
  Query 500: 18.20ms
  ...
  Query 1000: 17.40ms

Overall Improvement: +28.4%
  First window: 24.30ms
  Last window:  17.40ms

======================================================================
QUERY MONITORING SUMMARY
======================================================================
Total Queries:           1000
Cross-shard Queries:     450 (45.0%)
Local Queries:           550
Unique Nodes Accessed:   320
Hot Nodes (top 10%):     32
======================================================================

======================================================================
DYNAMIC PROXY MANAGEMENT STATISTICS
======================================================================
Evaluations Performed:   10
Promotions Executed:     87
Unique Nodes Replicated: 52
Total Actions:           87

Replications per Shard:
  Shard 0: 28
  Shard 1: 31
  Shard 2: 28
======================================================================
```

**What It Measures:**
- Query latency with adaptive optimization
- Latency improvement over time
- Optimization actions taken
- Access pattern analysis

---

## Comparing Results

Compare standard vs adaptive benchmarks:

```bash
python compare_benchmarks.py
```

**Example Output:**
```
======================================================================
STANDARD (STATIC) BENCHMARK ANALYSIS
======================================================================

Queries Executed:  100
Mean Latency:      24.50 ms
Median Latency:    22.00 ms
P95 Latency:       38.20 ms
P99 Latency:       45.60 ms

======================================================================
ADAPTIVE (DYNAMIC) BENCHMARK ANALYSIS
======================================================================

Queries Executed:      1000
Mean Latency:          19.30 ms
Median Latency:        17.50 ms
P95 Latency:           28.90 ms
P99 Latency:           35.20 ms

Optimization Actions:
  Evaluations:         10
  Promotions:          87
  Nodes Replicated:    52

Query Patterns:
  Cross-shard:         450 (45.0%)
  Hot Nodes:           32

Latency Improvement Over Time:
  Initial:             24.30 ms
  Final:               17.40 ms
  Change:              +28.4%

======================================================================
COMPARISON: STANDARD vs ADAPTIVE
======================================================================

Metric               Standard        Adaptive        Improvement
----------------------------------------------------------------------
Mean Latency         24.50 ms        19.30 ms        +21.2%
Median Latency       22.00 ms        17.50 ms        +20.5%
P95 Latency          38.20 ms        28.90 ms        +24.3%
P99 Latency          45.60 ms        35.20 ms        +22.8%
----------------------------------------------------------------------

Overall Mean Latency Improvement: +21.2%
✓ Adaptive approach shows SIGNIFICANT improvement!

✓ Comparison plot saved to ../results/benchmark_comparison.png
======================================================================
```

---

## Custom Configuration

### Example 1: Aggressive Optimization

For maximum performance (higher storage cost):

**Edit `config.py`:**
```python
# Use greedy strategy with high replication budget
MANAGEMENT_STRATEGY = 'greedy'
GREEDY_TOP_K = 20

# Frequent evaluations
EVALUATION_FREQUENCY = 50

# Allow more replications
MAX_REPLICATION_PERCENTAGE = 0.25  # 25%

# Prioritize latency
LATENCY_WEIGHT = 2.0
CROSS_SHARD_WEIGHT = 2.0

# Lower cost penalties
REPLICATION_COST_BASE = 2.0
```

**Run:**
```bash
python adaptive_benchmark.py
```

**Expected Results:**
- More nodes replicated (up to 25% per shard)
- Lower latency (more aggressive optimization)
- Higher storage usage

---

### Example 2: Conservative Optimization

For minimal storage overhead:

**Edit `config.py`:**
```python
# Use threshold strategy
MANAGEMENT_STRATEGY = 'threshold'
PROMOTION_THRESHOLD = 30.0  # High threshold

# Less frequent evaluations
EVALUATION_FREQUENCY = 200

# Limit replications
MAX_REPLICATION_PERCENTAGE = 0.08  # 8%

# Higher cost penalties
REPLICATION_COST_BASE = 15.0
REPLICATION_COST_PER_EDGE = 0.3
```

**Run:**
```bash
python adaptive_benchmark.py
```

**Expected Results:**
- Fewer nodes replicated (only very hot nodes)
- Modest latency improvement
- Minimal storage overhead

---

### Example 3: Balanced Approach (Recommended)

Default configuration provides good balance:

**`config.py` (default):**
```python
MANAGEMENT_STRATEGY = 'budget_aware'
EVALUATION_FREQUENCY = 100
MAX_REPLICATION_PERCENTAGE = 0.15  # 15%

# Balanced weights
FREQUENCY_WEIGHT = 1.0
CROSS_SHARD_WEIGHT = 2.0
LATENCY_WEIGHT = 1.5
COLOCATION_BONUS = 0.5
```

---

## Advanced Usage

### 1. Programmatic Usage

```python
from adaptive_benchmark import AdaptiveBenchmark
import config

# Customize configuration
config.MANAGEMENT_STRATEGY = 'budget_aware'
config.EVALUATION_FREQUENCY = 100
config.MAX_REPLICATION_PERCENTAGE = 0.15

# Create and run benchmark
benchmark = AdaptiveBenchmark(
    graph_file="../data/graph.pkl",
    partition_file="../data/refined_partition.pkl"
)

try:
    # Run benchmark
    benchmark.run_benchmark()

    # Analyze results
    benchmark.analyze_results()

    # Save results
    benchmark.save_results("../results/my_adaptive_results.pkl")

finally:
    # Clean up
    benchmark.cleanup()
```

### 2. Custom Query Patterns

```python
from query_monitor import QueryMonitor
from proxy_scorer import ProxyScorer
from dynamic_proxy_manager import DynamicProxyManager

# Initialize components
monitor = QueryMonitor()
monitor.load_partition_data("../data/refined_partition.pkl")

scorer = ProxyScorer(monitor)
scorer.load_graph("../data/graph.pkl")

manager = DynamicProxyManager(monitor, scorer)
manager.connect_to_neo4j()

# Simulate custom query pattern
for i in range(1000):
    # Your custom query logic here
    node_id = select_node_based_on_pattern(i)
    shard = determine_shard(node_id)

    # Execute query and record
    latency = execute_query(node_id)
    monitor.record_access(node_id, shard, latency, is_proxy=True)

    # Periodic optimization
    if i % 100 == 0:
        manager.evaluate_and_optimize()

# Analyze results
monitor.print_summary()
scorer.print_top_scores(k=10)
manager.print_statistics()
```

### 3. Real-time Monitoring

```python
from query_monitor import QueryMonitor

monitor = QueryMonitor()
monitor.load_partition_data()

# Run queries with monitoring
for query in workload:
    start = time.time()
    result = execute_query(query)
    latency = (time.time() - start) * 1000

    # Record access
    monitor.record_access(
        node_id=query.node_id,
        accessing_shard=query.shard,
        latency_ms=latency,
        is_proxy=True
    )

    # Real-time statistics
    if monitor.query_count % 50 == 0:
        stats = monitor.get_statistics()
        print(f"Queries: {stats['total_queries']}, "
              f"Avg Latency: {stats['avg_latency_ms']:.2f}ms, "
              f"Cross-shard: {stats['cross_shard_percentage']:.1f}%")

# Final analysis
monitor.print_summary()
hot_nodes = monitor.get_hot_nodes(percentile=90)
print(f"Top 10% hot nodes: {len(hot_nodes)}")
```

### 4. Analyzing Score Components

```python
from proxy_scorer import ProxyScorer

scorer = ProxyScorer(monitor)
all_scores = scorer.score_all_proxies()

# Analyze what drives scores
for shard_id, scores in all_scores.items():
    print(f"\n[Shard {shard_id}] Score Analysis:")

    for score in scores[:5]:  # Top 5
        print(f"\nNode {score.node_id}:")
        print(f"  Total Score: {score.score:.2f}")
        print(f"  Breakdown:")
        print(f"    Frequency:    {score.frequency_component:.2f}")
        print(f"    Cross-shard:  {score.cross_shard_component:.2f}")
        print(f"    Latency:      {score.latency_component:.2f}")
        print(f"    Co-location:  {score.colocation_component:.2f}")
        print(f"    Cost:         -{score.replication_cost:.2f}")
```

### 5. Pattern Detection

```python
from query_monitor import QueryMonitor

monitor = QueryMonitor()
# ... run queries ...

# Detect access patterns
patterns = monitor.detect_access_patterns()

for node_id, pattern_info in patterns.items():
    print(f"Node {node_id}: {pattern_info['type']} access pattern")
    print(f"  Avg interval: {pattern_info['avg_interval']:.2f}s")
    print(f"  Coefficient of variation: {pattern_info['cv']:.2f}")

# Use patterns to adjust strategy
steady_nodes = [nid for nid, p in patterns.items() if p['type'] == 'steady']
bursty_nodes = [nid for nid, p in patterns.items() if p['type'] == 'bursty']

print(f"\nSteady access: {len(steady_nodes)} nodes (good candidates for replication)")
print(f"Bursty access: {len(bursty_nodes)} nodes (maybe cache instead)")
```

---

## Troubleshooting Examples

### Issue: Neo4j Connection Failed

```bash
# Check if Neo4j is running
docker ps | grep neo4j

# If not running, start it
cd docker && docker-compose up -d

# Check logs for errors
docker logs neo4j-enterprise

# Test connection
python -c "
from neo4j import GraphDatabase
driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password123'))
with driver.session() as session:
    result = session.run('RETURN 1 as num')
    print('Connection OK:', result.single()['num'])
driver.close()
"
```

### Issue: Low Optimization Impact

```python
# Analyze why optimizations aren't helping

# 1. Check if enough queries are being run
from adaptive_benchmark import AdaptiveBenchmark
import config

# Increase query count
config.ADAPTIVE_BENCHMARK_QUERIES = 2000  # More queries

# 2. Check evaluation frequency
config.EVALUATION_FREQUENCY = 50  # More frequent

# 3. Analyze score distribution
scorer.print_score_analysis()

# 4. Check if nodes are actually being promoted
manager.print_statistics()

# If promotions are low:
config.PROMOTION_THRESHOLD = 10.0  # Lower threshold
config.MAX_REPLICATION_PERCENTAGE = 0.20  # Higher budget
```

---

## Next Steps

After running the examples:

1. **Experiment with configurations** - Try different strategies and parameters
2. **Analyze results** - Use comparison tools to measure improvements
3. **Optimize for your workload** - Adjust weights based on query patterns
4. **Monitor in production** - Track statistics and adapt as needed

For detailed documentation, see:
- [DYNAMIC_PROXY_MANAGEMENT.md](DYNAMIC_PROXY_MANAGEMENT.md) - Full technical documentation
- [README.md](README.md) - Project overview
- `config.py` - All tunable parameters
