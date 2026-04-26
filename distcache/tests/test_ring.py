"""Tests for ConsistentHashRing."""

import pytest
from cache.ring import ConsistentHashRing


def make_ring(node_names=("A","B","C"), vnodes=50):
    r = ConsistentHashRing(vnodes=vnodes)
    for n in node_names:
        r.add_node(n)
    return r


def test_routes_to_known_node():
    ring = make_ring()
    node = ring.get_node("some-key")
    assert node in ring.nodes


def test_stable_mapping():
    """Same key always maps to the same node."""
    ring = make_ring()
    node = ring.get_node("stable-key")
    for _ in range(20):
        assert ring.get_node("stable-key") == node


def test_minimal_remapping_on_add():
    """Adding 1 node should remap ~1/N keys, not all of them."""
    keys = [f"key:{i}" for i in range(2000)]
    ring = ConsistentHashRing(vnodes=100)
    for n in ("A","B","C"):
        ring.add_node(n)
    before = {k: ring.get_node(k) for k in keys}
    ring.add_node("D")
    moved = sum(1 for k in keys if before[k] != ring.get_node(k))
    fraction = moved / len(keys)
    # theoretical: ~1/4 = 25%; allow generous ±15%
    assert 0.10 < fraction < 0.40, f"unexpected remap fraction: {fraction:.1%}"


def test_minimal_remapping_on_remove():
    keys = [f"key:{i}" for i in range(2000)]
    ring = ConsistentHashRing(vnodes=100)
    for n in ("A","B","C","D"):
        ring.add_node(n)
    before = {k: ring.get_node(k) for k in keys}
    ring.remove_node("D")
    moved = sum(1 for k in keys if before[k] != ring.get_node(k))
    fraction = moved / len(keys)
    # theoretical: ~1/4 of keys were on D, now moved; allow ±15%
    assert 0.10 < fraction < 0.40, f"unexpected remap fraction: {fraction:.1%}"


def test_replication_returns_distinct_nodes():
    ring = ConsistentHashRing(vnodes=50, replicas=3)
    for n in ("A","B","C","D"):
        ring.add_node(n)
    nodes = ring.get_nodes("mykey", count=3)
    assert len(nodes) == 3
    assert len(set(nodes)) == 3  # all distinct


def test_empty_ring_returns_none():
    ring = ConsistentHashRing()
    assert ring.get_node("anything") is None


def test_add_remove_idempotent():
    ring = make_ring()
    ring.add_node("A")   # already exists
    assert ring.nodes.count("A") == 1
    ring.remove_node("Z")  # not in ring, should not raise


def test_load_balance():
    """Each node should own roughly 1/N of keys."""
    ring = ConsistentHashRing(vnodes=200)
    for n in ("A","B","C"):
        ring.add_node(n)
    keys = [f"key:{i}" for i in range(3000)]
    counts = {n: 0 for n in ring.nodes}
    for k in keys:
        counts[ring.get_node(k)] += 1
    expected = len(keys) / len(ring)
    for node, count in counts.items():
        assert 0.5 * expected < count < 1.5 * expected, (
            f"{node} has {count} keys, expected ~{expected:.0f}")
