import argparse
import multiprocessing as mp
import random
import sys

# Ensure imports are handled correctly from your local project structure
try:
    from analysis.scaling import run_scaling_demo
    from analysis.hotkey import run_hotkey_demo
except ImportError:
    print("Error: Ensure 'analysis', 'cache', and 'workload' modules are in your path.")
    sys.exit(1)

def demo_cluster(n_nodes: int) -> None:
    """Live multi-process cache cluster demo."""
    from cache.cluster import CacheCluster
    from workload.generators import uniform_workload, skewed_workload

    print("\n" + "=" * 60)
    print(f"CLUSTER DEMO — Live Multi-process Cache ({n_nodes} nodes)")
    print("=" * 60)

    cluster = CacheCluster(vnodes=150, replicas=1, capacity=128)
    node_names = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa"]
    
    # Initialize with the number of nodes requested via CLI
    for name in node_names[:n_nodes]:
        cluster.add_node(name)

    # 1. Uniform Workload
    n = 300
    print(f"\n-- Uniform workload ({n} ops) --")
    keys = uniform_workload(n, seed=42)
    for k in keys:
        cluster.put(k, f"val:{k}")
    
    hits = sum(1 for k in keys if cluster.get(k) is not None)
    print(f"  Hit rate: {hits}/{n} = {hits/n:.1%}")
    for s in cluster.stats():
        print(f"  {s['node']:10s}  size={s['size']:3d}  "
              f"hits={s['hits']:4d}  misses={s['misses']:4d}  "
              f"hit_rate={s['hit_rate']:.1%}")

    # 2. Scaling Event
    # Check if delta is already added; if not, add it to demonstrate rebalancing
    if "delta" not in node_names[:n_nodes]:
        print(f"\n-- Adding node 'delta' --")
        cluster.add_node("delta")

    # 3. Skewed Workload (Hot-Key Analysis)
    hot_key = "key:hot"
    print(f"\n-- Skewed workload ({n} ops, hot_key={hot_key!r} @80%) --")
    skewed = skewed_workload(n, hot_key=hot_key, seed=42)
    for k in skewed:
        cluster.put(k, f"val:{k}")
    
    hits2 = sum(1 for k in skewed if cluster.get(k) is not None)
    print(f"  Overall hit rate: {hits2}/{n} = {hits2/n:.1%}")
    
    hot_node = cluster.ring.get_node(hot_key)
    print(f"  Hot key '{hot_key}' → node '{hot_node}'")
    
    for s in cluster.stats():
        marker = " ← HOT NODE" if s["node"] == hot_node else ""
        print(f"  {s['node']:10s}  size={s['size']:3d}  "
              f"hits={s['hits']:4d}  misses={s['misses']:4d}  "
              f"hit_rate={s['hit_rate']:.1%}{marker}")

    cluster.shutdown()
    print("\n[cluster] All nodes shut down.")

def main() -> None:
    parser = argparse.ArgumentParser(description="Project 15: Distributed Cache Demos")
    parser.add_argument(
        "--demo",
        choices=["scaling", "hotkey", "cluster", "all"],
        default="all",
        help="The specific demo to run"
    )
    parser.add_argument(
        "--nodes", type=int, default=3,
        help="Initial number of cache nodes to spawn"
    )
    args = parser.parse_args()

    random.seed(42)

    if args.demo in ("scaling", "all"):
        run_scaling_demo()

    if args.demo in ("hotkey", "all"):
        run_hotkey_demo()

    if args.demo in ("cluster", "all"):
        # Use 'spawn' for cross-platform compatibility (required for macOS/Windows)
        try:
            mp.set_start_method("spawn", force=True)
        except RuntimeError:
            pass 
        demo_cluster(n_nodes=args.nodes)

if __name__ == "__main__":
    main()