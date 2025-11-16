"""
Amortization Tracking Framework
Addresses Feedback: Amortization analysis, cost-benefit tracking
"""

import time
import pickle
import numpy as np
from collections import defaultdict
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt

class AmortizationTracker:
    """
    Tracks cumulative costs and benefits of dynamic repartitioning
    Computes ROI, break-even points, and amortized costs
    """
    
    def __init__(self):
        # Repartitioning costs
        self.repartition_events = []  # List of repartition overhead times
        self.consistency_windows = []  # List of consistency window times
        
        # Query performance
        self.query_latencies_before = []  # Latencies before repartition
        self.query_latencies_after = []   # Latencies after repartition
        
        # Cumulative metrics
        self.total_repartition_overhead = 0.0  # ms
        self.total_consistency_overhead = 0.0  # ms
        self.total_queries_executed = 0
        self.cumulative_latency_saved = 0.0    # ms
        
        # Per-repartition tracking
        self.repartition_metrics = []
        
        # Start time
        self.start_time = time.time()
    
    def record_repartition_overhead(self, compute_time_ms, consistency_window_ms):
        """
        Record overhead from a repartitioning event
        
        Args:
            compute_time_ms: Time to compute new partition
            consistency_window_ms: Time for consistency-aware migration
        """
        total_overhead = compute_time_ms + consistency_window_ms
        
        self.repartition_events.append({
            'timestamp': time.time() - self.start_time,
            'compute_time': compute_time_ms,
            'consistency_window': consistency_window_ms,
            'total_overhead': total_overhead
        })
        
        self.total_repartition_overhead += total_overhead
        self.total_consistency_overhead += consistency_window_ms
    
    def record_query_latencies(self, latencies, before_repartition=True):
        """
        Record query latencies
        
        Args:
            latencies: List of query latencies in ms
            before_repartition: True if before last repartition, False if after
        """
        if before_repartition:
            self.query_latencies_before.extend(latencies)
        else:
            self.query_latencies_after.extend(latencies)
        
        self.total_queries_executed += len(latencies)
    
    def compute_cumulative_latency_saved(self):
        """
        Compute cumulative latency saved:
        S_total = Σ_q (L_before(q) - L_after(q))
        
        Returns:
            Total latency saved in ms
        """
        if not self.query_latencies_before or not self.query_latencies_after:
            return 0.0
        
        avg_before = np.mean(self.query_latencies_before)
        avg_after = np.mean(self.query_latencies_after)
        
        # Assume all queries after repartition benefit from improvement
        queries_after = len(self.query_latencies_after)
        latency_saved = (avg_before - avg_after) * queries_after
        
        self.cumulative_latency_saved = latency_saved
        return latency_saved
    
    def compute_roi(self):
        """
        Compute Return on Investment:
        ROI = (S_total - O_total) / O_total
        
        Returns:
            ROI value (positive means beneficial)
        """
        savings = self.compute_cumulative_latency_saved()
        overhead = self.total_repartition_overhead
        
        if overhead == 0:
            return 0.0
        
        roi = (savings - overhead) / overhead
        return roi
    
    def compute_break_even_queries(self):
        """
        Compute break-even point:
        N_breakeven = O_total / E[latency_reduction_per_query]
        
        Returns:
            Number of queries needed to break even
        """
        if not self.query_latencies_before or not self.query_latencies_after:
            return float('inf')
        
        avg_before = np.mean(self.query_latencies_before)
        avg_after = np.mean(self.query_latencies_after)
        
        avg_reduction = avg_before - avg_after
        
        if avg_reduction <= 0:
            return float('inf')  # Never breaks even
        
        break_even = self.total_repartition_overhead / avg_reduction
        return int(np.ceil(break_even))
    
    def compute_amortized_cost_per_query(self):
        """
        Compute amortized cost per query:
        A_query = O_total / N_queries
        
        Returns:
            Amortized overhead per query in ms
        """
        if self.total_queries_executed == 0:
            return 0.0
        
        amortized = self.total_repartition_overhead / self.total_queries_executed
        return amortized
    
    def record_repartition_cycle(self, cycle_num):
        """
        Record metrics at end of repartition cycle
        """
        roi = self.compute_roi()
        break_even = self.compute_break_even_queries()
        amortized = self.compute_amortized_cost_per_query()
        
        metrics = {
            'cycle': cycle_num,
            'timestamp': time.time() - self.start_time,
            'total_overhead': self.total_repartition_overhead,
            'cumulative_savings': self.cumulative_latency_saved,
            'net_benefit': self.cumulative_latency_saved - self.total_repartition_overhead,
            'roi': roi,
            'break_even_queries': break_even,
            'queries_executed': self.total_queries_executed,
            'amortized_cost_per_query': amortized
        }
        
        self.repartition_metrics.append(metrics)
        return metrics
    
    def print_amortization_summary(self):
        """Print comprehensive amortization analysis"""
        roi = self.compute_roi()
        break_even = self.compute_break_even_queries()
        amortized = self.compute_amortized_cost_per_query()
        savings = self.cumulative_latency_saved
        overhead = self.total_repartition_overhead
        net_benefit = savings - overhead
        
        print(f"\n{'='*70}")
        print("AMORTIZATION ANALYSIS")
        print(f"{'='*70}")
        
        print("\n1. OVERHEAD COSTS:")
        print(f"   Total Repartition Overhead:    {overhead:,.2f} ms")
        print(f"   - Computation Time:            {overhead - self.total_consistency_overhead:,.2f} ms")
        print(f"   - Consistency Window Time:     {self.total_consistency_overhead:,.2f} ms")
        print(f"   Number of Repartition Events:  {len(self.repartition_events)}")
        print(f"   Avg Overhead per Event:        {overhead/max(len(self.repartition_events),1):,.2f} ms")
        
        print("\n2. PERFORMANCE BENEFITS:")
        print(f"   Queries Executed:              {self.total_queries_executed:,}")
        print(f"   Avg Latency Before:            {np.mean(self.query_latencies_before) if self.query_latencies_before else 0:.2f} ms")
        print(f"   Avg Latency After:             {np.mean(self.query_latencies_after) if self.query_latencies_after else 0:.2f} ms")
        print(f"   Latency Improvement:           {np.mean(self.query_latencies_before) - np.mean(self.query_latencies_after) if self.query_latencies_before and self.query_latencies_after else 0:.2f} ms/query")
        print(f"   Cumulative Latency Saved:      {savings:,.2f} ms")
        
        print("\n3. AMORTIZATION METRICS:")
        print(f"   Net Benefit:                   {net_benefit:,.2f} ms")
        print(f"   ROI:                           {roi:.2%}")
        print(f"   Break-Even Point:              {break_even:,} queries")
        print(f"   Queries Past Break-Even:       {max(0, self.total_queries_executed - break_even):,}")
        print(f"   Amortized Cost per Query:      {amortized:.4f} ms")
        
        print("\n4. COST-BENEFIT VERDICT:")
        if roi > 0:
            print(f"   ✓ POSITIVE ROI - Dynamic repartitioning is BENEFICIAL")
            print(f"   ✓ System has executed {self.total_queries_executed - break_even:,} queries past break-even")
            print(f"   ✓ Net time saved: {net_benefit:,.2f} ms ({net_benefit/1000:.2f} seconds)")
        else:
            print(f"   ✗ NEGATIVE ROI - Need {break_even - self.total_queries_executed:,} more queries to break even")
            print(f"   ✗ Consider: reducing repartition frequency or overhead")
        
        print(f"{'='*70}\n")
    
    def plot_amortization_curves(self, save_path='../results/amortization_plot.png'):
        """Generate visualization of amortization over time"""
        if not self.repartition_metrics:
            print("No repartition metrics to plot")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        cycles = [m['cycle'] for m in self.repartition_metrics]
        overheads = [m['total_overhead'] for m in self.repartition_metrics]
        savings = [m['cumulative_savings'] for m in self.repartition_metrics]
        net_benefits = [m['net_benefit'] for m in self.repartition_metrics]
        rois = [m['roi'] for m in self.repartition_metrics]
        
        # Plot 1: Cumulative Costs vs Savings
        axes[0, 0].plot(cycles, overheads, 'r-', label='Overhead', linewidth=2)
        axes[0, 0].plot(cycles, savings, 'g-', label='Savings', linewidth=2)
        axes[0, 0].fill_between(cycles, 0, overheads, alpha=0.3, color='red')
        axes[0, 0].fill_between(cycles, 0, savings, alpha=0.3, color='green')
        axes[0, 0].set_xlabel('Repartition Cycle')
        axes[0, 0].set_ylabel('Time (ms)')
        axes[0, 0].set_title('Cumulative Overhead vs Savings')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
        
        # Plot 2: Net Benefit Over Time
        axes[0, 1].plot(cycles, net_benefits, 'b-', linewidth=2, marker='o')
        axes[0, 1].axhline(y=0, color='k', linestyle='--', alpha=0.5)
        axes[0, 1].fill_between(cycles, 0, net_benefits, 
                                where=np.array(net_benefits) >= 0, 
                                alpha=0.3, color='green', label='Positive')
        axes[0, 1].fill_between(cycles, 0, net_benefits,
                                where=np.array(net_benefits) < 0,
                                alpha=0.3, color='red', label='Negative')
        axes[0, 1].set_xlabel('Repartition Cycle')
        axes[0, 1].set_ylabel('Net Benefit (ms)')
        axes[0, 1].set_title('Net Benefit (Savings - Overhead)')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)
        
        # Plot 3: ROI Over Time
        axes[1, 0].plot(cycles, rois, 'purple', linewidth=2, marker='s')
        axes[1, 0].axhline(y=0, color='k', linestyle='--', alpha=0.5)
        axes[1, 0].set_xlabel('Repartition Cycle')
        axes[1, 0].set_ylabel('ROI')
        axes[1, 0].set_title('Return on Investment')
        axes[1, 0].grid(True, alpha=0.3)
        
        # Plot 4: Amortized Cost per Query
        amortized_costs = [m['amortized_cost_per_query'] for m in self.repartition_metrics]
        axes[1, 1].plot(cycles, amortized_costs, 'orange', linewidth=2, marker='^')
        axes[1, 1].set_xlabel('Repartition Cycle')
        axes[1, 1].set_ylabel('Amortized Cost (ms/query)')
        axes[1, 1].set_title('Amortized Overhead per Query')
        axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"✓ Amortization plot saved to {save_path}")
        plt.close()
    
    def save_metrics(self, path='../results/amortization_metrics.pkl'):
        """Save all metrics to file"""
        data = {
            'repartition_events': self.repartition_events,
            'query_latencies_before': self.query_latencies_before,
            'query_latencies_after': self.query_latencies_after,
            'repartition_metrics': self.repartition_metrics,
            'summary': {
                'total_overhead': self.total_repartition_overhead,
                'total_savings': self.cumulative_latency_saved,
                'roi': self.compute_roi(),
                'break_even_queries': self.compute_break_even_queries(),
                'total_queries': self.total_queries_executed
            }
        }
        
        with open(path, 'wb') as f:
            pickle.dump(data, f)
        
        print(f"✓ Metrics saved to {path}")


if __name__ == "__main__":
    print("Testing Amortization Tracker...\n")
    
    tracker = AmortizationTracker()
    
    # Simulate repartitioning events
    tracker.record_repartition_overhead(compute_time_ms=3000, consistency_window_ms=500)
    
    # Simulate queries before repartition
    latencies_before = np.random.normal(100, 15, 500)  # 100ms avg, 15ms std
    tracker.record_query_latencies(latencies_before, before_repartition=True)
    
    # Simulate queries after repartition (improved)
    latencies_after = np.random.normal(83, 12, 500)  # 83ms avg, 12ms std
    tracker.record_query_latencies(latencies_after, before_repartition=False)
    
    # Record cycle metrics
    tracker.record_repartition_cycle(cycle_num=1)
    
    # Print summary
    tracker.print_amortization_summary()
    
    print("✓ Amortization tracker test complete!")