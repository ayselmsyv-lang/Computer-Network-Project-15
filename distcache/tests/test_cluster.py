"""Tests for CacheCluster (multiprocessing)."""

import multiprocessing as mp
import pytest
from cache.cluster import CacheCluster


@pytest.fixture(scope="module", autouse=True)
def mp_spawn():
    mp.set_start_method("spawn", force=True)


@pytest.fixture
def cluster():
    c = CacheCluster(vnodes=50, replicas=1, capacity=64)
    for name in ("alpha", "beta", "gamma"):
        c.add_node(name)
    yield c
    c.shutdown()


def test_put_and_get(cluster):
    cluster.put("hello", "world")
    assert cluster.get("hello") == "world"


def test_get_miss(cluster):
    assert cluster.get("no-such-key-xyz") is None


def test_stats_have_all_nodes(cluster):
    stats = cluster.stats()
    names = {s["node"] for s in stats}
    assert names == {"alpha", "beta", "gamma"}


def test_add_node_expands_ring(cluster):
    before = len(cluster.ring)
    cluster.add_node("delta")
    assert len(cluster.ring) == before + 1
    cluster.remove_node("delta")


def test_remove_node_shrinks_ring(cluster):
    cluster.add_node("temp")
    before = len(cluster.ring)
    cluster.remove_node("temp")
    assert len(cluster.ring) == before - 1


def test_multiple_puts_and_gets(cluster):
    pairs = {f"k{i}": f"v{i}" for i in range(50)}
    for k, v in pairs.items():
        cluster.put(k, v)
    hits = sum(1 for k, v in pairs.items() if cluster.get(k) == v)
    # LRU may evict some; expect at least 80% hit
    assert hits / len(pairs) >= 0.8
