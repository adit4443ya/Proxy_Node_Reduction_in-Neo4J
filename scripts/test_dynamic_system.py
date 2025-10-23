"""
Test Script for Dynamic Proxy Management System

This script verifies that all components are working correctly.
Run this after installing dependencies (pip install -r requirements.txt).
"""

import sys
from typing import Tuple


def test_imports() -> Tuple[bool, str]:
    """Test that all modules can be imported"""
    print("\n" + "="*70)
    print("TEST 1: Module Imports")
    print("="*70)

    modules = [
        'config',
        'query_monitor',
        'proxy_scorer',
        'dynamic_proxy_manager',
        'adaptive_benchmark',
        'compare_benchmarks'
    ]

    try:
        for module_name in modules:
            __import__(module_name)
            print(f"✓ {module_name}")

        return True, "All modules imported successfully"
    except ImportError as e:
        return False, f"Import error: {e}"


def test_query_monitor() -> Tuple[bool, str]:
    """Test QueryMonitor functionality"""
    print("\n" + "="*70)
    print("TEST 2: QueryMonitor")
    print("="*70)

    try:
        from query_monitor import QueryMonitor

        monitor = QueryMonitor()
        print("✓ QueryMonitor initialized")

        # Add test data
        monitor.node_to_shard = {i: i % 3 for i in range(10)}

        # Record some accesses
        for i in range(20):
            monitor.record_access(
                node_id=i % 10,
                accessing_shard=(i % 3),
                latency_ms=10.0 + i,
                is_proxy=True
            )

        print(f"✓ Recorded {monitor.query_count} accesses")

        # Get statistics
        stats = monitor.get_statistics()
        print(f"✓ Statistics: {stats['total_queries']} queries, "
              f"{stats['cross_shard_percentage']:.1f}% cross-shard")

        # Get hot nodes
        hot_nodes = monitor.get_hot_nodes(percentile=50)
        print(f"✓ Identified {len(hot_nodes)} hot nodes")

        return True, "QueryMonitor tests passed"
    except Exception as e:
        return False, f"QueryMonitor error: {e}"


def test_proxy_scorer() -> Tuple[bool, str]:
    """Test ProxyScorer functionality"""
    print("\n" + "="*70)
    print("TEST 3: ProxyScorer")
    print("="*70)

    try:
        from query_monitor import QueryMonitor
        from proxy_scorer import ProxyScorer

        # Create monitor with test data
        monitor = QueryMonitor()
        monitor.node_to_shard = {i: i % 3 for i in range(20)}

        # Simulate queries
        for _ in range(50):
            monitor.record_access(node_id=5, accessing_shard=0, latency_ms=15.0, is_proxy=True)

        for _ in range(30):
            monitor.record_access(node_id=8, accessing_shard=1, latency_ms=18.0, is_proxy=True)

        print(f"✓ Created test query pattern ({monitor.query_count} queries)")

        # Create scorer
        scorer = ProxyScorer(monitor)
        print("✓ ProxyScorer initialized")

        # Score proxies
        all_scores = scorer.score_all_proxies()
        total_candidates = sum(len(scores) for scores in all_scores.values())
        print(f"✓ Scored proxies: {total_candidates} candidates")

        # Get top K
        top_scores = scorer.get_top_k_per_shard(k=3)
        print(f"✓ Top-K selection: {sum(len(s) for s in top_scores.values())} scores")

        return True, "ProxyScorer tests passed"
    except Exception as e:
        return False, f"ProxyScorer error: {e}"


def test_dynamic_manager() -> Tuple[bool, str]:
    """Test DynamicProxyManager (without Neo4j connection)"""
    print("\n" + "="*70)
    print("TEST 4: DynamicProxyManager (offline)")
    print("="*70)

    try:
        from query_monitor import QueryMonitor
        from proxy_scorer import ProxyScorer
        from dynamic_proxy_manager import DynamicProxyManager

        # Create components
        monitor = QueryMonitor()
        monitor.node_to_shard = {i: i % 3 for i in range(20)}

        # Simulate queries
        for _ in range(50):
            monitor.record_access(node_id=5, accessing_shard=0, latency_ms=15.0, is_proxy=True)

        scorer = ProxyScorer(monitor)
        print("✓ Created monitor and scorer")

        # Create manager (without Neo4j connection)
        manager = DynamicProxyManager(monitor, scorer)
        print("✓ DynamicProxyManager initialized")

        # Get statistics
        stats = manager.get_statistics()
        print(f"✓ Manager statistics: {stats['evaluation_count']} evaluations, "
              f"{stats['promotions_executed']} promotions")

        return True, "DynamicProxyManager tests passed (Neo4j operations skipped)"
    except Exception as e:
        return False, f"DynamicProxyManager error: {e}"


def test_configuration() -> Tuple[bool, str]:
    """Test configuration module"""
    print("\n" + "="*70)
    print("TEST 5: Configuration")
    print("="*70)

    try:
        import config

        # Check key parameters
        params = [
            ('MANAGEMENT_STRATEGY', config.MANAGEMENT_STRATEGY),
            ('EVALUATION_FREQUENCY', config.EVALUATION_FREQUENCY),
            ('MAX_REPLICATION_PERCENTAGE', config.MAX_REPLICATION_PERCENTAGE),
            ('PROMOTION_THRESHOLD', config.PROMOTION_THRESHOLD),
        ]

        for name, value in params:
            print(f"✓ {name} = {value}")

        return True, "Configuration tests passed"
    except Exception as e:
        return False, f"Configuration error: {e}"


def run_all_tests():
    """Run all tests and print summary"""
    print("\n" + "="*70)
    print("DYNAMIC PROXY MANAGEMENT SYSTEM - TEST SUITE")
    print("="*70)

    tests = [
        ("Module Imports", test_imports),
        ("QueryMonitor", test_query_monitor),
        ("ProxyScorer", test_proxy_scorer),
        ("DynamicProxyManager", test_dynamic_manager),
        ("Configuration", test_configuration),
    ]

    results = []

    for test_name, test_func in tests:
        try:
            success, message = test_func()
            results.append((test_name, success, message))
        except Exception as e:
            results.append((test_name, False, f"Unexpected error: {e}"))

    # Print summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)

    passed = sum(1 for _, success, _ in results if success)
    total = len(results)

    for test_name, success, message in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status:8} | {test_name:30} | {message}")

    print("="*70)
    print(f"Results: {passed}/{total} tests passed")
    print("="*70)

    if passed == total:
        print("\n🎉 All tests passed! System is ready to use.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    # Check dependencies
    try:
        import networkx
        import neo4j
        import numpy
    except ImportError as e:
        print("\n" + "="*70)
        print("DEPENDENCY ERROR")
        print("="*70)
        print(f"\n✗ Missing required dependency: {e}")
        print("\nPlease install dependencies first:")
        print("  pip install -r requirements.txt")
        print("\n" + "="*70)
        sys.exit(1)

    # Run tests
    exit_code = run_all_tests()
    sys.exit(exit_code)
