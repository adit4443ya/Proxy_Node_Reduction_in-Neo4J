"""
Predictive Workload-Aware Repartitioning (PWAR)
Addresses Feedback: How is Δt defined? Need adaptive mechanisms
"""

import numpy as np
import pickle
from collections import defaultdict, deque
import time

class WorkloadPredictor:
    """
    Predicts future edge access patterns using Exponentially Weighted Moving Average (EWMA)
    """
    
    def __init__(self, alpha_ewma=0.3, window_size=5):
        """
        Args:
            alpha_ewma: Smoothing factor for EWMA (0 < α < 1)
            window_size: Number of historical observations to keep
        """
        self.alpha_ewma = alpha_ewma
        self.window_size = window_size
        
        # Edge weight history: edge -> deque of (timestamp, weight)
        self.weight_history = defaultdict(lambda: deque(maxlen=window_size))
        
        # Predicted weights: edge -> predicted weight
        self.predicted_weights = {}
        
        # Workload volatility metrics
        self.volatility_history = deque(maxlen=window_size)
    
    def update_observed_weights(self, edge_weights, timestamp=None):
        """
        Update observed edge weights
        
        Args:
            edge_weights: Dict of (u,v) -> weight
            timestamp: Optional timestamp (default: current time)
        """
        if timestamp is None:
            timestamp = time.time()
        
        for edge, weight in edge_weights.items():
            self.weight_history[edge].append((timestamp, weight))
    
    def predict_edge_weight(self, edge):
        """
        Predict future weight for an edge using EWMA:
        w_pred(e,t) = α·w_obs(e,t) + (1-α)·w_pred(e,t-1)
        
        Args:
            edge: Tuple (u, v)
        
        Returns:
            Predicted weight
        """
        history = self.weight_history.get(edge, [])
        
        if not history:
            return 1.0  # Default weight
        
        # Get most recent observed weight
        _, w_obs = history[-1]
        
        # Get previous prediction
        w_prev = self.predicted_weights.get(edge, w_obs)
        
        # EWMA update
        w_pred = self.alpha_ewma * w_obs + (1 - self.alpha_ewma) * w_prev
        
        self.predicted_weights[edge] = w_pred
        return w_pred
    
    def predict_all_weights(self, graph):
        """
        Predict weights for all edges in graph
        
        Returns:
            Dict of (u,v) -> predicted weight
        """
        predicted = {}
        for u, v in graph.edges():
            # Try both edge directions
            w_uv = self.predict_edge_weight((u, v))
            w_vu = self.predict_edge_weight((v, u))
            
            # Use maximum of both directions
            predicted[(u, v)] = max(w_uv, w_vu)
        
        return predicted
    
    def compute_workload_volatility(self):
        """
        Compute Coefficient of Variation (CV) of workload:
        CV = σ(W) / μ(W)
        
        Returns:
            Workload volatility (CV)
        """
        if not self.weight_history:
            return 0.0
        
        # Collect all recent weights
        recent_weights = []
        for edge, history in self.weight_history.items():
            if history:
                _, weight = history[-1]
                recent_weights.append(weight)
        
        if not recent_weights:
            return 0.0
        
        mean_weight = np.mean(recent_weights)
        std_weight = np.std(recent_weights)
        
        # Coefficient of Variation
        cv = std_weight / mean_weight if mean_weight > 0 else 0.0
        
        self.volatility_history.append(cv)
        return cv
    
    def get_average_volatility(self):
        """Get smoothed volatility over recent history"""
        if not self.volatility_history:
            return 0.0
        return np.mean(self.volatility_history)


class AdaptiveRepartitionController:
    """
    Controls adaptive repartitioning with dynamic thresholds and time windows
    """
    
    def __init__(self, theta_base=0.1, delta_t_base=600, beta_vol=0.5, gamma_vol=0.3):
        """
        Args:
            theta_base: Base repartition threshold (default: 0.1 = 10%)
            delta_t_base: Base time window in seconds (default: 600s = 10min)
            beta_vol: Volatility influence on threshold (default: 0.5)
            gamma_vol: Volatility influence on time window (default: 0.3)
        """
        self.theta_base = theta_base
        self.delta_t_base = delta_t_base
        self.beta_vol = beta_vol
        self.gamma_vol = gamma_vol
        
        self.workload_predictor = WorkloadPredictor()
        
        # Metrics tracking
        self.repartition_events = []
        self.cost_history = []
    
    def compute_adaptive_threshold(self, volatility):
        """
        Adaptive threshold:
        θ_adaptive = θ_base · (1 + β_vol · CV)
        
        Higher volatility → higher threshold (more conservative)
        """
        theta = self.theta_base * (1 + self.beta_vol * volatility)
        return theta
    
    def compute_adaptive_time_window(self, volatility):
        """
        Adaptive time window:
        Δt_adaptive = Δt_base / (1 + γ_vol · CV)
        
        Higher volatility → shorter window (more responsive)
        """
        delta_t = self.delta_t_base / (1 + self.gamma_vol * volatility)
        return delta_t
    
    def should_repartition(self, current_cost, previous_cost, volatility):
        """
        Determine if repartitioning should be triggered
        
        Uses adaptive threshold based on workload volatility
        """
        if previous_cost == 0:
            return False
        
        cost_change = abs(current_cost - previous_cost) / previous_cost
        
        theta_adaptive = self.compute_adaptive_threshold(volatility)
        
        should_trigger = cost_change > theta_adaptive
        
        print(f"\n{'='*60}")
        print("ADAPTIVE REPARTITION DECISION")
        print(f"{'='*60}")
        print(f"  Workload Volatility (CV):     {volatility:.4f}")
        print(f"  Base Threshold θ_base:         {self.theta_base:.4f}")
        print(f"  Adaptive Threshold θ_adaptive: {theta_adaptive:.4f}")
        print(f"  Cost Change:                   {cost_change:.4f}")
        print(f"  Decision:                      {'REPARTITION' if should_trigger else 'SKIP'}")
        print(f"{'='*60}\n")
        
        return should_trigger
    
    def get_next_time_window(self, volatility):
        """Get adaptive time window for next iteration"""
        delta_t = self.compute_adaptive_time_window(volatility)
        
        print(f"Next check in {delta_t:.1f} seconds")
        print(f"  (Base: {self.delta_t_base}s, Volatility: {volatility:.4f})")
        
        return delta_t
    
    def record_repartition_event(self, timestamp, cost_before, cost_after, volatility):
        """Record repartitioning event for analysis"""
        event = {
            'timestamp': timestamp,
            'cost_before': cost_before,
            'cost_after': cost_after,
            'cost_reduction': cost_before - cost_after,
            'volatility': volatility,
            'adaptive_threshold': self.compute_adaptive_threshold(volatility),
            'adaptive_window': self.compute_adaptive_time_window(volatility)
        }
        self.repartition_events.append(event)
    
    def print_repartition_summary(self):
        """Print summary of all repartitioning events"""
        if not self.repartition_events:
            print("No repartitioning events recorded")
            return
        
        print("\n" + "="*60)
        print("REPARTITIONING EVENT SUMMARY")
        print("="*60)
        
        for i, event in enumerate(self.repartition_events, 1):
            print(f"\nEvent {i}:")
            print(f"  Timestamp:          {event['timestamp']:.2f}")
            print(f"  Cost Before:        {event['cost_before']:.4f}")
            print(f"  Cost After:         {event['cost_after']:.4f}")
            print(f"  Cost Reduction:     {event['cost_reduction']:.4f}")
            print(f"  Volatility:         {event['volatility']:.4f}")
            print(f"  Adaptive Threshold: {event['adaptive_threshold']:.4f}")
            print(f"  Adaptive Window:    {event['adaptive_window']:.1f}s")
        
        total_reduction = sum(e['cost_reduction'] for e in self.repartition_events)
        avg_volatility = np.mean([e['volatility'] for e in self.repartition_events])
        
        print(f"\nAggregate Statistics:")
        print(f"  Total Events:           {len(self.repartition_events)}")
        print(f"  Total Cost Reduction:   {total_reduction:.4f}")
        print(f"  Average Volatility:     {avg_volatility:.4f}")
        print("="*60 + "\n")


if __name__ == "__main__":
    # Test adaptive controller
    print("Testing Adaptive Repartition Controller...\n")
    
    controller = AdaptiveRepartitionController()
    
    # Simulate different volatility scenarios
    test_scenarios = [
        (0.1, "Low volatility - stable workload"),
        (0.5, "Medium volatility - evolving workload"),
        (1.0, "High volatility - chaotic workload")
    ]
    
    for volatility, description in test_scenarios:
        print(f"\nScenario: {description}")
        print(f"  Volatility CV = {volatility}")
        
        theta = controller.compute_adaptive_threshold(volatility)
        delta_t = controller.compute_adaptive_time_window(volatility)
        
        print(f"  Adaptive Threshold: {theta:.4f} (base: {controller.theta_base})")
        print(f"  Adaptive Window: {delta_t:.1f}s (base: {controller.delta_t_base}s)")
    
    print("\n✓ Adaptive controller test complete!")