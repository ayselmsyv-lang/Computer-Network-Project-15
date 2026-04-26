"""
Hot-Key Analysis
----------------
Demonstrates that uniform consistent hashing does NOT prevent hotspots,
and shows mitigation via key replication + round-robin reads.
"""

import collections
import statistics

from cache.ring import ConsistentHashRing
from workload.generators import skewed_workload


def node_loads(ring: ConsistentHashRing, workload: list[str]) -> dict[str, int]:
    return dict(collections.Counter(ring.get_node(k) for k in workload))


def replicated_loads(ring: ConsistentHashRing,
                     workload: list[str],
                     replicas: int) -> dict[str, int]:
    """Spread reads across replicas via per-key round-robin."""
    counts: dict[str, int] = {n: 0 for n in ring.nodes}
    rr: dict[str, int] = {}
    for k in workload:
        nodes = ring.get_nodes(k, replicas)
        idx = rr.get(k, 0) % len(nodes)
        counts[nodes[idx]] = counts.get(nodes[idx], 0) + 1
        rr[k] = idx + 1
    return counts


def print_load_bar(counts: dict[str, int], total: int,
                   hot_node: str | None = None, width: int = 40) -> None:
    for node, count in sorted(counts.items()):
        pct = count / total if total else 0
        bar = "█" * int(pct * width)
        marker = " ← HOT" if node == hot_node else ""
        print(f"  {node:10s} {count:5d}  ({pct:.1%})  {bar}{marker}")


def run_hotkey_demo(n_requests: int = 5000,
                    hot_key: str = "key:hot",
                    hot_fraction: float = 0.8,
                    replicas: int = 3) -> None:
    print("\n" + "=" * 60)
    print("HOT-KEY DEMO — Skew Detection & Replication Mitigation")
    print("=" * 60)

    ring = ConsistentHashRing(vnodes=150, replicas=1)
    for name in ["nodeA", "nodeB", "nodeC"]:
        ring.add_node(name)

    workload = skewed_workload(n_requests, hot_key=hot_key,
                               hot_fraction=hot_fraction, seed=42)
    hot_node = ring.get_node(hot_key)
    total = len(workload)

    print(f"\nWorkload : {n_requests} requests, "
          f"hot_key={hot_key!r} @ {hot_fraction:.0%}")
    print(f"Hot key  → {hot_node}")

    # Without replication
    counts = node_loads(ring, workload)
    print(f"\nWithout replication:")
    print_load_bar(counts, total, hot_node)
    print(f"  std-dev: {statistics.stdev(counts.values()):.1f}")

    # With replication
    rep_ring = ConsistentHashRing(vnodes=150, replicas=replicas)
    for name in ["nodeA", "nodeB", "nodeC"]:
        rep_ring.add_node(name)
    hot_nodes = rep_ring.get_nodes(hot_key, replicas)
    rep_counts = replicated_loads(rep_ring, workload, replicas)

    print(f"\nWith replication (factor={replicas}, round-robin reads):")
    print(f"Hot key replicated to: {hot_nodes}")
    print_load_bar(rep_counts, total)
    print(f"  std-dev: {statistics.stdev(rep_counts.values()):.1f}")
    print("  (lower std-dev = more balanced)")
