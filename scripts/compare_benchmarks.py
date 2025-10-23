"""
Comparison Tool for Standard vs Adaptive Benchmarks

This script runs both the standard (static) and adaptive (dynamic) benchmarks
and compares their performance.
"""

import pickle
import time
from statistics import mean, median
from typing import Dict, List
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt


def load_benchmark_results(filepath: str) -> Dict:
    """Load benchmark results from pickle file"""
    try:
        with open(filepath, 'rb') as f:
            return pickle.load(f)
    except Exception as e:
        print(f"✗ Failed to load {filepath}: {e}")
        return None


def analyze_standard_benchmark(filepath: str = "../results/benchmark_results.pkl"):
    """Analyze standard benchmark results"""
    print("\n" + "=" * 70)
    print("STANDARD (STATIC) BENCHMARK ANALYSIS")
    print("=" * 70)

    data = load_benchmark_results(filepath)
    if not data:
        return None

    latencies = data.get('neighbor_latencies', [])
    result_counts = data.get('result_counts', [])

    if not latencies:
        print("✗ No latency data found")
        return None

    stats = {
        'mean_latency': mean(latencies),
        'median_latency': median(latencies),
        'min_latency': min(latencies),
        'max_latency': max(latencies),
        'total_queries': len(latencies),
        'avg_results': mean(result_counts) if result_counts else 0
    }

    # Calculate percentiles
    sorted_latencies = sorted(latencies)
    stats['p95_latency'] = sorted_latencies[int(len(sorted_latencies) * 0.95)]
    stats['p99_latency'] = sorted_latencies[int(len(sorted_latencies) * 0.99)]

    print(f"\nQueries Executed:  {stats['total_queries']}")
    print(f"Mean Latency:      {stats['mean_latency']:.2f} ms")
    print(f"Median Latency:    {stats['median_latency']:.2f} ms")
    print(f"P95 Latency:       {stats['p95_latency']:.2f} ms")
    print(f"P99 Latency:       {stats['p99_latency']:.2f} ms")
    print(f"Min Latency:       {stats['min_latency']:.2f} ms")
    print(f"Max Latency:       {stats['max_latency']:.2f} ms")
    print(f"Avg Results:       {stats['avg_results']:.1f}")

    return stats


def analyze_adaptive_benchmark(filepath: str = "../results/adaptive_benchmark_results.pkl"):
    """Analyze adaptive benchmark results"""
    print("\n" + "=" * 70)
    print("ADAPTIVE (DYNAMIC) BENCHMARK ANALYSIS")
    print("=" * 70)

    data = load_benchmark_results(filepath)
    if not data:
        return None

    query_results = data.get('query_results', [])
    if not query_results:
        print("✗ No query results found")
        return None

    latencies = [r['latency_ms'] for r in query_results]
    result_counts = [r['result_count'] for r in query_results]

    stats = {
        'mean_latency': mean(latencies),
        'median_latency': median(latencies),
        'min_latency': min(latencies),
        'max_latency': max(latencies),
        'total_queries': len(latencies),
        'avg_results': mean(result_counts) if result_counts else 0
    }

    # Calculate percentiles
    sorted_latencies = sorted(latencies)
    stats['p95_latency'] = sorted_latencies[int(len(sorted_latencies) * 0.95)]
    stats['p99_latency'] = sorted_latencies[int(len(sorted_latencies) * 0.99)]

    # Optimization stats
    manager_stats = data.get('manager_stats', {})
    monitor_stats = data.get('monitor_stats', {})

    print(f"\nQueries Executed:      {stats['total_queries']}")
    print(f"Mean Latency:          {stats['mean_latency']:.2f} ms")
    print(f"Median Latency:        {stats['median_latency']:.2f} ms")
    print(f"P95 Latency:           {stats['p95_latency']:.2f} ms")
    print(f"P99 Latency:           {stats['p99_latency']:.2f} ms")
    print(f"Min Latency:           {stats['min_latency']:.2f} ms")
    print(f"Max Latency:           {stats['max_latency']:.2f} ms")
    print(f"Avg Results:           {stats['avg_results']:.1f}")

    if manager_stats:
        print(f"\nOptimization Actions:")
        print(f"  Evaluations:         {manager_stats.get('evaluation_count', 0)}")
        print(f"  Promotions:          {manager_stats.get('promotions_executed', 0)}")
        print(f"  Nodes Replicated:    {manager_stats.get('unique_nodes_replicated', 0)}")

    if monitor_stats:
        print(f"\nQuery Patterns:")
        print(f"  Cross-shard:         {monitor_stats.get('cross_shard_queries', 0)} "
              f"({monitor_stats.get('cross_shard_percentage', 0):.1f}%)")
        print(f"  Hot Nodes:           {monitor_stats.get('hot_nodes_count', 0)}")

    # Analyze trend
    latency_windows = data.get('latency_windows', [])
    if len(latency_windows) >= 2:
        first_window = latency_windows[0]['avg_latency']
        last_window = latency_windows[-1]['avg_latency']
        improvement = ((first_window - last_window) / first_window) * 100

        print(f"\nLatency Improvement Over Time:")
        print(f"  Initial:             {first_window:.2f} ms")
        print(f"  Final:               {last_window:.2f} ms")
        print(f"  Change:              {improvement:+.1f}%")

        stats['improvement_percentage'] = improvement
        stats['latency_windows'] = latency_windows

    return stats


def compare_benchmarks(standard_stats: Dict, adaptive_stats: Dict):
    """Compare standard vs adaptive benchmark results"""
    print("\n" + "=" * 70)
    print("COMPARISON: STANDARD vs ADAPTIVE")
    print("=" * 70)

    metrics = [
        ('Mean Latency', 'mean_latency', 'ms'),
        ('Median Latency', 'median_latency', 'ms'),
        ('P95 Latency', 'p95_latency', 'ms'),
        ('P99 Latency', 'p99_latency', 'ms'),
    ]

    print(f"\n{'Metric':<20} {'Standard':<15} {'Adaptive':<15} {'Improvement':<15}")
    print("-" * 70)

    for metric_name, metric_key, unit in metrics:
        standard_val = standard_stats.get(metric_key, 0)
        adaptive_val = adaptive_stats.get(metric_key, 0)

        if standard_val > 0:
            improvement = ((standard_val - adaptive_val) / standard_val) * 100
            improvement_str = f"{improvement:+.1f}%"
        else:
            improvement_str = "N/A"

        print(f"{metric_name:<20} {standard_val:>10.2f} {unit:<3} "
              f"{adaptive_val:>10.2f} {unit:<3} {improvement_str:>15}")

    print("-" * 70)

    # Overall assessment
    mean_improvement = ((standard_stats['mean_latency'] - adaptive_stats['mean_latency']) /
                       standard_stats['mean_latency']) * 100

    print(f"\nOverall Mean Latency Improvement: {mean_improvement:+.1f}%")

    if mean_improvement > 5:
        print("✓ Adaptive approach shows SIGNIFICANT improvement!")
    elif mean_improvement > 0:
        print("✓ Adaptive approach shows modest improvement")
    elif mean_improvement > -5:
        print("~ Performance is similar")
    else:
        print("✗ Standard approach performed better")

    return mean_improvement


def plot_comparison(standard_stats: Dict, adaptive_stats: Dict,
                   output_file: str = "../results/benchmark_comparison.png"):
    """Create visualization comparing benchmarks"""
    try:
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('Standard vs Adaptive Benchmark Comparison', fontsize=16, fontweight='bold')

        # 1. Latency metrics comparison
        ax1 = axes[0, 0]
        metrics = ['Mean', 'Median', 'P95', 'P99']
        standard_vals = [
            standard_stats['mean_latency'],
            standard_stats['median_latency'],
            standard_stats['p95_latency'],
            standard_stats['p99_latency']
        ]
        adaptive_vals = [
            adaptive_stats['mean_latency'],
            adaptive_stats['median_latency'],
            adaptive_stats['p95_latency'],
            adaptive_stats['p99_latency']
        ]

        x = range(len(metrics))
        width = 0.35
        ax1.bar([i - width/2 for i in x], standard_vals, width, label='Standard', alpha=0.8)
        ax1.bar([i + width/2 for i in x], adaptive_vals, width, label='Adaptive', alpha=0.8)
        ax1.set_ylabel('Latency (ms)')
        ax1.set_title('Latency Metrics Comparison')
        ax1.set_xticks(x)
        ax1.set_xticklabels(metrics)
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)

        # 2. Improvement percentages
        ax2 = axes[0, 1]
        improvements = [
            ((s - a) / s * 100) for s, a in zip(standard_vals, adaptive_vals)
        ]
        colors = ['green' if imp > 0 else 'red' for imp in improvements]
        ax2.bar(metrics, improvements, color=colors, alpha=0.7)
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax2.set_ylabel('Improvement (%)')
        ax2.set_title('Latency Improvement by Metric')
        ax2.grid(axis='y', alpha=0.3)

        # 3. Latency trend over time (if available)
        ax3 = axes[1, 0]
        if 'latency_windows' in adaptive_stats:
            windows = adaptive_stats['latency_windows']
            query_indices = [w['query_index'] for w in windows]
            avg_latencies = [w['avg_latency'] for w in windows]

            ax3.plot(query_indices, avg_latencies, marker='o', linewidth=2, markersize=6)
            ax3.set_xlabel('Query Index')
            ax3.set_ylabel('Avg Latency (ms)')
            ax3.set_title('Adaptive Benchmark: Latency Trend Over Time')
            ax3.grid(True, alpha=0.3)

            # Add trend line
            if len(query_indices) > 1:
                z = np.polyfit(query_indices, avg_latencies, 1)
                p = np.poly1d(z)
                ax3.plot(query_indices, p(query_indices), "--", alpha=0.5, color='red',
                        label=f'Trend (slope: {z[0]:.3f})')
                ax3.legend()
        else:
            ax3.text(0.5, 0.5, 'No trend data available', ha='center', va='center',
                    transform=ax3.transAxes)
            ax3.set_title('Latency Trend (N/A)')

        # 4. Summary statistics
        ax4 = axes[1, 1]
        ax4.axis('off')

        summary_text = f"""
        SUMMARY STATISTICS

        Standard Benchmark:
          • Queries: {standard_stats['total_queries']}
          • Mean Latency: {standard_stats['mean_latency']:.2f} ms
          • P95 Latency: {standard_stats['p95_latency']:.2f} ms

        Adaptive Benchmark:
          • Queries: {adaptive_stats['total_queries']}
          • Mean Latency: {adaptive_stats['mean_latency']:.2f} ms
          • P95 Latency: {adaptive_stats['p95_latency']:.2f} ms

        Overall Improvement:
          • Mean Latency: {((standard_stats['mean_latency'] - adaptive_stats['mean_latency']) / standard_stats['mean_latency'] * 100):+.1f}%
          • P95 Latency: {((standard_stats['p95_latency'] - adaptive_stats['p95_latency']) / standard_stats['p95_latency'] * 100):+.1f}%
        """

        ax4.text(0.1, 0.9, summary_text, fontsize=10, verticalalignment='top',
                fontfamily='monospace', transform=ax4.transAxes)

        plt.tight_layout()
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"\n✓ Comparison plot saved to {output_file}")

    except Exception as e:
        print(f"\n✗ Failed to create plot: {e}")


def main():
    """Main comparison function"""
    print("=" * 70)
    print("BENCHMARK COMPARISON TOOL")
    print("=" * 70)

    # Analyze both benchmarks
    standard_stats = analyze_standard_benchmark()
    adaptive_stats = analyze_adaptive_benchmark()

    if not standard_stats or not adaptive_stats:
        print("\n✗ Cannot compare - missing benchmark data")
        print("  Run both benchmarks first:")
        print("    python benchmark_queries.py")
        print("    python adaptive_benchmark.py")
        return

    # Compare results
    improvement = compare_benchmarks(standard_stats, adaptive_stats)

    # Create visualization
    try:
        import numpy as np
        plot_comparison(standard_stats, adaptive_stats)
    except ImportError:
        print("\n! Matplotlib/NumPy not available - skipping visualization")

    print("\n" + "=" * 70)
    print("COMPARISON COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
