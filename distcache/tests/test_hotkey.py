"""Tests for hot-key analysis utilities."""

import statistics
from analysis.hotkey import node_loads, replicated_loads
from analysis.scaling import load_distribution, measure_remapping, snapshot_mapping
from cache.ring import ConsistentHashRing
from workload.generators import uniform_workload, skewed_workload


def make_ring(nodes=("A","B","C"), vnodes=100):
    r = ConsistentHashRing(vnodes=vnodes)
    for n in nodes:
        r.add_node(n)
    return r


def test_uniform_workload_length():
    wl = uniform_workload(500, seed=1)
    assert len(wl) == 500


def test_skewed_hot_key_fraction():
    hot = "key:hot"
    wl = skewed_workload(2000, hot_key=hot, hot_fraction=0.7, seed=42)
    actual = wl.count(hot) / len(wl)
    assert 0.60 < actual < 0.80


def test_replication_reduces_std():
    ring = make_ring()
    hot = "key:hot"
    wl = skewed_workload(3000, hot_key=hot, hot_fraction=0.8, seed=7)

    no_rep = node_loads(ring, wl)
    rep_ring = ConsistentHashRing(vnodes=100, replicas=3)
    for n in ("A","B","C"):
        rep_ring.add_node(n)
    with_rep = replicated_loads(rep_ring, wl, 3)

    std_no  = statistics.stdev(no_rep.values())
    std_rep = statistics.stdev(with_rep.values())
    assert std_rep < std_no, "replication should reduce load std-dev"


def test_measure_remapping_no_change():
    ring = make_ring()
    keys = [f"key:{i}" for i in range(500)]
    snap = snapshot_mapping(ring, keys)
    result = measure_remapping(snap, snap, len(ring))
    assert result["remapped"] == 0
    assert result["fraction"] == 0.0


def test_load_distribution_sums_to_sample():
    ring = make_ring()
    keys = [f"key:{i}" for i in range(300)]
    dist = load_distribution(ring, keys)
    assert sum(dist.values()) == 300
