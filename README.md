# Proxy Node Reduction in Neo4j

A comprehensive system for optimizing distributed graph databases through intelligent graph partitioning, proxy node management, and **dynamic adaptive optimization**.

## Features

### Core Features
- 🎯 **Multi-tenant Graph Generation**: Realistic synthetic social network graphs with Zipf distribution
- 📊 **METIS-based Partitioning**: Minimize edge-cuts across shards
- 🔧 **Proxy-Aware Refinement**: Iterative optimization to reduce proxy node overhead
- 💾 **Multi-Database Neo4j**: Single instance with multiple shard databases
- 🔍 **Composite Database Queries**: Unified query interface across shards
- 📈 **Performance Benchmarking**: Measure query latency and proxy impact

### 🚀 NEW: Dynamic Proxy Management
- 📡 **Real-time Query Monitoring**: Track access patterns across shards
- 🎯 **Intelligent Proxy Scoring**: Identify high-value replication candidates
- ⚡ **Adaptive Optimization**: Dynamically promote/replicate proxy nodes
- 📊 **Performance Analysis**: Compare static vs adaptive approaches

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- Neo4j 5.15+ (Enterprise)

### Installation

```bash
# Clone repository
git clone <repo-url>
cd Proxy_Node_Reduction_in-Neo4J

# Install Python dependencies
pip install -r requirements.txt
```

**Required Python packages:**
```
networkx
pymetis
neo4j
numpy
matplotlib (optional, for visualizations)
```

### Basic Workflow (Static Partitioning)

```bash
# 1. Start Neo4j container
cd docker && docker-compose up -d

# 2. Create databases
cd ../scripts
python setup_databases.py

# 3. Generate and partition graph
python generate_graph.py      # Creates multi-tenant graph
python partition_graph.py     # METIS partitioning
python proxy_refinement.py    # Optimize proxy placement

# 4. Load data to Neo4j
python load_to_neo4j.py       # Load to shard1, shard2, shard3
python setup_composite.py     # Create composite database

# 5. Run benchmark
python benchmark_queries.py   # Standard benchmark
```

### Advanced Workflow (Dynamic Optimization)

```bash
# Run steps 1-4 from basic workflow, then:

# 5. Run adaptive benchmark with dynamic proxy management
python adaptive_benchmark.py

# 6. Compare standard vs adaptive performance
python compare_benchmarks.py
```

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                     Neo4j Ecosystem                          │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                     │
│  │ shard1  │  │ shard2  │  │ shard3  │  ← Partitioned DBs  │
│  └────┬────┘  └────┬────┘  └────┬────┘                     │
│       └───────────┬─────────────┘                            │
│                   │                                           │
│           ┌───────▼────────┐                                 │
│           │  mycomposite   │  ← Unified query interface      │
│           └───────┬────────┘                                 │
│                   │                                           │
│           ┌───────▼─────────────────────────────┐           │
│           │   Dynamic Proxy Management          │           │
│           │   • Query Monitor                    │           │
│           │   • Proxy Scorer                     │           │
│           │   • Adaptive Optimizer               │           │
│           └─────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
[Graph Generation]
    ↓
[METIS Partitioning] → metis_partition.pkl
    ↓
[Proxy Refinement] → refined_partition.pkl
    ↓
[Load to Neo4j] → shard1, shard2, shard3
    ↓
[Create Composite] → mycomposite
    ↓
┌──────────────────────────────────────┐
│ Standard Benchmark  │ Adaptive Bench │
│  (Static proxies)   │ (Dynamic opt)  │
└──────────────────────────────────────┘
    ↓                       ↓
[benchmark_results.pkl] [adaptive_results.pkl]
    ↓                       ↓
    └──────────┬────────────┘
               ↓
    [compare_benchmarks.py]
```

---

## Project Structure

```
Proxy_Node_Reduction_in-Neo4J/
├── docker/
│   └── docker-compose.yaml          # Neo4j container setup
│
├── scripts/
│   ├── generate_graph.py            # Multi-tenant graph generation
│   ├── partition_graph.py           # METIS partitioning
│   ├── proxy_refinement.py          # Proxy-aware optimization
│   ├── setup_databases.py           # Create Neo4j databases
│   ├── load_to_neo4j.py            # Load partitions to Neo4j
│   ├── setup_composite.py           # Create composite database
│   ├── benchmark_queries.py         # Standard benchmark
│   │
│   ├── config.py                    # 🆕 Configuration parameters
│   ├── query_monitor.py             # 🆕 Query pattern tracking
│   ├── proxy_scorer.py              # 🆕 Proxy node scoring
│   ├── dynamic_proxy_manager.py     # 🆕 Adaptive optimization
│   ├── adaptive_benchmark.py        # 🆕 Dynamic benchmark
│   └── compare_benchmarks.py        # 🆕 Performance comparison
│
├── data/
│   ├── graph.pkl                    # Generated graph
│   ├── metis_partition.pkl          # Initial partition
│   ├── refined_partition.pkl        # Optimized partition
│   ├── monitoring_stats.pkl         # 🆕 Query monitoring data
│   └── optimization_history.pkl     # 🆕 Optimization actions
│
├── results/
│   ├── benchmark_results.pkl        # Standard benchmark results
│   ├── adaptive_benchmark_results.pkl # 🆕 Adaptive results
│   └── benchmark_comparison.png     # 🆕 Comparison visualization
│
├── README.md                         # This file
└── DYNAMIC_PROXY_MANAGEMENT.md      # 🆕 Detailed feature docs
```

---

## Dynamic Proxy Management

### Overview

Traditional static partitioning creates proxy nodes at load time but never adjusts them. The **dynamic proxy management system** monitors query patterns and adaptively optimizes proxy placement to reduce latency.

### Key Concepts

#### 1. Query Monitoring
Tracks which nodes are accessed from which shards:
```python
monitor.record_access(
    node_id=42,
    accessing_shard=0,
    latency_ms=15.0,
    is_proxy=True
)
```

#### 2. Proxy Scoring
Calculates replication benefit:
```
score = (frequency × cross_shard_ratio × latency_impact)
        + colocation_bonus - replication_cost
```

#### 3. Dynamic Management
Three strategies available:
- **Greedy**: Top-K highest scoring proxies
- **Threshold**: Score-based promotion triggers
- **Budget-Aware**: Constrained optimization (recommended)

### Quick Example

```python
from adaptive_benchmark import AdaptiveBenchmark

# Run adaptive benchmark
benchmark = AdaptiveBenchmark()
benchmark.run_benchmark()
benchmark.analyze_results()
```

### Configuration

Edit `scripts/config.py`:

```python
# Strategy selection
MANAGEMENT_STRATEGY = 'budget_aware'  # or 'greedy', 'threshold'

# Optimization frequency
EVALUATION_FREQUENCY = 100  # Optimize every 100 queries

# Replication budget
MAX_REPLICATION_PERCENTAGE = 0.15  # Max 15% replicated nodes

# Scoring weights
FREQUENCY_WEIGHT = 1.0
CROSS_SHARD_WEIGHT = 2.0
LATENCY_WEIGHT = 1.5
```

For detailed documentation, see: **[DYNAMIC_PROXY_MANAGEMENT.md](DYNAMIC_PROXY_MANAGEMENT.md)**

---

## Benchmarking

### Standard Benchmark

Static partitioning with fixed proxy nodes:

```bash
python benchmark_queries.py
```

**Metrics:**
- Query latency (mean, median, P95, P99)
- Result counts
- Proxy node percentages

### Adaptive Benchmark

Dynamic optimization with live monitoring:

```bash
python adaptive_benchmark.py
```

**Metrics:**
- Query latency trends over time
- Optimization actions taken
- Cross-shard query reduction
- Nodes replicated per shard

### Comparison

```bash
python compare_benchmarks.py
```

**Output:**
- Side-by-side statistics
- Latency improvement percentages
- Visualization plots
- Optimization summary

Example output:
```
Metric               Standard        Adaptive        Improvement
---------------------------------------------------------------------
Mean Latency         24.50 ms        19.30 ms        +21.2%
Median Latency       22.00 ms        17.50 ms        +20.5%
P95 Latency          38.20 ms        28.90 ms        +24.3%
P99 Latency          45.60 ms        35.20 ms        +22.8%
```

---

## Configuration Reference

### Graph Generation

```python
# In generate_graph.py
NUM_TENANTS = 100              # Number of tenant subgraphs
MIN_TENANT_SIZE = 50           # Minimum nodes per tenant
TENANT_SIZE_ALPHA = 1.5        # Zipf distribution parameter
BA_MODEL_M = 3                 # Barabási-Albert parameter
```

### Partitioning

```python
# In partition_graph.py
NUM_PARTITIONS = 3             # Number of shards (k-way)
METIS_SEED = 42               # Random seed for reproducibility
```

### Proxy Refinement

```python
# In proxy_refinement.py
MAX_ITERATIONS = 10            # Refinement iterations
BALANCE_TOLERANCE = 0.05       # ±5% partition size tolerance
BRIDGE_VERTEX_PERCENTILE = 0.1 # Top 10% cross-partition nodes
```

### Neo4j Connection

```python
# In config.py
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"
```

---

## Performance Optimization

### Tuning for Latency

```python
# Prioritize latency reduction
LATENCY_WEIGHT = 2.0
EVALUATION_FREQUENCY = 50
MAX_REPLICATION_PERCENTAGE = 0.20
```

### Tuning for Storage

```python
# Minimize storage overhead
REPLICATION_COST_BASE = 10.0
MAX_REPLICATION_PERCENTAGE = 0.10
PROMOTION_THRESHOLD = 20.0
```

### Tuning for Throughput

```python
# Maximize query throughput
FREQUENCY_WEIGHT = 2.0
CROSS_SHARD_WEIGHT = 2.0
MANAGEMENT_STRATEGY = 'greedy'
```

---

## Troubleshooting

### Docker Issues

```bash
# Check Neo4j status
docker ps

# View Neo4j logs
docker logs neo4j-enterprise

# Restart container
docker-compose restart
```

### Database Connection Issues

```python
# Test connection
from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "password123")
)

with driver.session() as session:
    result = session.run("RETURN 1 as num")
    print(result.single()['num'])  # Should print: 1
```

### No Proxies Being Promoted

Check monitoring stats:
```python
monitor.print_summary()
scorer.print_top_scores(k=10)
```

Solutions:
- Lower `PROMOTION_THRESHOLD`
- Increase `MAX_REPLICATION_PERCENTAGE`
- Decrease `MIN_QUERIES_THRESHOLD`

### Performance Not Improving

Possible causes:
1. Wrong optimization strategy
2. Insufficient evaluation frequency
3. Query pattern doesn't match scoring weights

Solutions:
- Analyze score components
- Try different strategies
- Adjust scoring weights

---

## Algorithm Details

### METIS Partitioning

Uses multilevel k-way partitioning:
1. **Coarsening**: Iteratively merge nodes
2. **Initial Partitioning**: Partition coarsest graph
3. **Uncoarsening**: Project back with refinement

**Objective**: Minimize edge-cut while balancing partition sizes

### Proxy-Aware Refinement

```python
for iteration in range(MAX_ITERATIONS):
    # Identify bridge vertices (high cross-partition degree)
    candidates = find_bridge_vertices()

    for node in candidates:
        # Evaluate migration benefit
        for target_partition in partitions:
            gain = compute_migration_gain(node, target_partition)

            if gain > 0 and satisfies_balance_constraint():
                migrate(node, target_partition)
```

**Gain Calculation:**
```
proxy_reduction = current_proxies - new_proxies
balance_penalty = 10 if violates_balance else 0
gain = proxy_reduction - balance_penalty
```

### Dynamic Proxy Scoring

```python
def score_proxy(node, target_shard, stats):
    # Access frequency
    freq = stats.access_count / time_window * 60  # per minute

    # Cross-shard cost
    cross_ratio = stats.cross_shard_accesses / stats.total_accesses

    # Latency impact
    latency_norm = stats.avg_latency / baseline_latency

    # Co-location benefit
    coloc = neighbors_in_shard / total_neighbors

    # Replication cost
    cost = base_cost + (degree * cost_per_edge)

    # Final score
    return (freq * cross_ratio * latency_norm + coloc) - cost
```

---

## Research Applications

This system is useful for researching:

1. **Distributed Graph Databases**
   - Proxy node optimization strategies
   - Cross-partition query patterns
   - Dynamic data placement

2. **Graph Partitioning**
   - Multi-objective optimization
   - Adaptive refinement algorithms
   - Tenant-aware partitioning

3. **Query Optimization**
   - Access pattern analysis
   - Predictive replication
   - Cost-based optimization

4. **Database Systems**
   - Multi-database architectures
   - Composite query federation
   - Adaptive indexing analogues

---

## Contributing

Contributions welcome! Areas for improvement:

- [ ] Additional optimization strategies
- [ ] Machine learning-based scoring
- [ ] Temporal pattern detection
- [ ] Multi-objective optimization
- [ ] Cost models for cloud deployments
- [ ] Visualization dashboards
- [ ] Additional benchmark workloads

---

## License

[Specify license here]

---

## References

### Technologies Used
- **Neo4j 5.15**: Graph database with multi-database support
- **METIS**: Fast graph partitioning library
- **NetworkX**: Python graph analysis library
- **Python 3.8+**: Implementation language

### Related Papers
- Karypis & Kumar (1998): "A Fast and High Quality Multilevel Scheme for Partitioning Irregular Graphs" (METIS)
- Graph partitioning in distributed databases
- Adaptive caching and replication strategies
- Query workload analysis in graph databases

---

## Support

For issues and questions:
- Review [DYNAMIC_PROXY_MANAGEMENT.md](DYNAMIC_PROXY_MANAGEMENT.md) for detailed documentation
- Check troubleshooting section above
- Review configuration parameters in `scripts/config.py`

---

**Built with ❤️ for distributed graph database optimization**
