"""
Scaling Analysis
----------------
Measures and reports key remapping when nodes join or leave the ring.
Validates the minimal remapping property of consistent hashing.
"""

import statistics
from cache.ring import ConsistentHashRing


def snapshot_mapping(ring: ConsistentHashRing,
                     sample_keys: list[str]) -> dict[str, str]:
    """Return {key: node} for all sample keys on the current ring."""
    return {k: ring.get_node(k) for k in sample_keys}


def measure_remapping(before: dict[str, str],
                      after: dict[str, str],
                      n_nodes_after: int) -> dict:
    """
    Compare two key->node snapshots.

    Returns:
        total       - number of keys sampled
        remapped    - keys whose node assignment changed
        fraction    - remapped / total
        expected    - theoretical 1/N_after
    """
    moved = sum(1 for k in before if before[k] != after.get(k))
    total = len(before)
    return {
        "total": total,
        "remapped": moved,
        "fraction": moved / total if total else 0.0,
        "expected": 1.0 / n_nodes_after if n_nodes_after else 0.0,
    }


def load_distribution(ring: ConsistentHashRing,
                      sample_keys: list[str]) -> dict[str, int]:
    """Count how many sample keys each node owns."""
    counts: dict[str, int] = {n: 0 for n in ring.nodes}
    for k in sample_keys:
        node = ring.get_node(k)
        if node:
            counts[node] = counts.get(node, 0) + 1
    return counts


def print_load_bar(distribution: dict[str, int],
                   total: int, width: int = 40) -> None:
    for node, count in sorted(distribution.items()):
        pct = count / total if total else 0
        bar = "█" * int(pct * width)
        print(f"  {node:12s} {count:5d}  ({pct:.1%})  {bar}")


def run_scaling_demo(sample_size: int = 5000, vnodes: int = 150) -> None:
    print("\n" + "=" * 60)
    print("SCALING DEMO — Key Remapping on Join / Leave")
    print("=" * 60)

    sample_keys = [f"key:{i}" for i in range(sample_size)]
    ring = ConsistentHashRing(vnodes=vnodes)

    for name in ["nodeA", "nodeB", "nodeC"]:
        ring.add_node(name)

    dist = load_distribution(ring, sample_keys)
    print(f"\nInitial ring: {ring.nodes}")
    print(f"Load ({sample_size} keys):")
    print_load_bar(dist, sample_size)
    std = statistics.stdev(dist.values())
    print(f"  std-dev: {std:.1f} keys")

    # --- add nodeD ---
    before = snapshot_mapping(ring, sample_keys)
    ring.add_node("nodeD")
    after = snapshot_mapping(ring, sample_keys)
    result = measure_remapping(before, after, len(ring))

    print(f"\nAfter adding nodeD:")
    print(f"  Remapped : {result['remapped']:,} / {result['total']:,} "
          f"({result['fraction']:.1%})")
    print(f"  Expected : ~{result['expected']:.1%}  (1/{len(ring)} nodes)")
    dist = load_distribution(ring, sample_keys)
    print_load_bar(dist, sample_size)

    # --- remove nodeD ---
    before2 = snapshot_mapping(ring, sample_keys)
    ring.remove_node("nodeD")
    after2 = snapshot_mapping(ring, sample_keys)
    result2 = measure_remapping(before2, after2, len(ring))

    print(f"\nAfter removing nodeD:")
    print(f"  Remapped : {result2['remapped']:,} / {result2['total']:,} "
          f"({result2['fraction']:.1%})")
    print(f"  Expected : ~{result2['expected']:.1%}  (1/{len(ring)} nodes)")
    dist = load_distribution(ring, sample_keys)
    print_load_bar(dist, sample_size)
