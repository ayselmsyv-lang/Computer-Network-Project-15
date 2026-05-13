import psutil
import os
import hashlib
import random
import collections
import statistics
import streamlit as st
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from visuals import plot_hash_ring, plot_node_distribution
from metrics import distribution_dataframe

st.set_page_config(page_title="Distributed Cache Simulator", layout="wide")

st.markdown("""
<style>
.stApp { background-color: #050816; }
h1, h2, h3 { color: white; }
</style>
""", unsafe_allow_html=True)

st.title("Distributed Cache Infrastructure Simulator")
st.caption("Consistent Hashing • Virtual Nodes • Hot-Key Analysis • Distributed Scaling")

# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.header("Simulation Settings")
st.sidebar.metric("Runtime Physical CPU Cores", psutil.cpu_count(logical=False))
st.sidebar.metric("Runtime Logical CPU Threads", os.cpu_count())

node_count = st.sidebar.slider("Node Count", 2, 10, 4)
vnodes     = st.sidebar.slider("Virtual Nodes", 1, 300, 150)
workload   = st.sidebar.selectbox("Workload", ["Uniform", "Zipfian"])

nodes = [f"Node-{i}" for i in range(1, node_count + 1)]

# ── Shared helpers ────────────────────────────────────────────────────────────
def build_ring(node_list, vn):
    ring = {}
    for node in node_list:
        for v in range(vn):
            h = int(hashlib.md5(f"{node}:{v}".encode()).hexdigest(), 16)
            ring[h] = node
    return ring

def get_node_from_ring(ring, key):
    sorted_keys = sorted(ring.keys())
    h = int(hashlib.md5(key.encode()).hexdigest(), 16)
    for rk in sorted_keys:
        if h <= rk:
            return ring[rk]
    return ring[sorted_keys[0]]

def compute_distribution(node_list, vn, wl, n_keys=1000):
    ring = build_ring(node_list, vn)
    counts = {n: 0 for n in node_list}
    if wl == "Uniform":
        keys = [f"key:{i}" for i in range(n_keys)]
    else:
        random.seed(42)
        hot  = [f"key:{i}" for i in range(n_keys // 5)]
        cold = [f"key:{i}" for i in range(n_keys // 5, n_keys)]
        keys = random.choices(hot, k=int(n_keys * 0.8)) + \
               random.choices(cold, k=int(n_keys * 0.2))
    for key in keys:
        counts[get_node_from_ring(ring, key)] += 1
    return counts

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["🗂 Cluster Overview", "📈 Scaling Analysis", "🔥 Hot-Key Analysis"])

# ═══════════════════════════════════════════════════════════════════
# TAB 1 — Cluster Overview
# ═══════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Consistent Hash Ring")
    ring_fig = plot_hash_ring(nodes, vnodes)
    st.pyplot(ring_fig)

    st.subheader("Node Load Distribution")
    distribution = compute_distribution(nodes, vnodes, workload)
    df = distribution_dataframe(distribution)

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df, use_container_width=True)
    with col2:
        st.pyplot(plot_node_distribution(distribution))
    
    if workload == "Zipfian":
        hot_node = max(distribution, key=distribution.get)
        hot_pct  = distribution[hot_node] / sum(distribution.values()) * 100
        st.error(f"Hot-key bottleneck detected on {hot_node} — {hot_pct:.1f}% of all traffic")

# ═══════════════════════════════════════════════════════════════════
# TAB 2 — Scaling Analysis
# ═══════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Key Remapping on Node Join / Leave")
    st.markdown("Validates the **minimal remapping** property of consistent hashing.")

    sample_keys = [f"key:{i}" for i in range(1000)]

    # Add node
    before_ring = build_ring(nodes, vnodes)
    new_node    = f"Node-{node_count + 1}"
    after_ring  = build_ring(nodes + [new_node], vnodes)

    remapped_add = sum(
        1 for k in sample_keys
        if get_node_from_ring(before_ring, k) != get_node_from_ring(after_ring, k)
    )
    pct_add      = remapped_add / len(sample_keys) * 100
    expected_add = 1 / (node_count + 1) * 100

    # Remove node
    remove_node  = nodes[-1]
    after_remove = build_ring(nodes[:-1], vnodes)
    remapped_rem = sum(
        1 for k in sample_keys
        if get_node_from_ring(before_ring, k) != get_node_from_ring(after_remove, k)
    )
    pct_rem      = remapped_rem / len(sample_keys) * 100
    expected_rem = 1 / max(node_count - 1, 1) * 100

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Virtual Nodes per Node", vnodes)
        st.metric(f"Keys Remapped — Add {new_node}",
                  f"{pct_add:.1f}%", f"Expected ~{expected_add:.1f}%")
        st.metric(f"Keys Remapped — Remove {remove_node}",
                  f"{pct_rem:.1f}%", f"Expected ~{expected_rem:.1f}%")

    with col2:
        # Load before/after chart
        dist_before = compute_distribution(nodes, vnodes, "Uniform")
        dist_after  = compute_distribution(nodes + [new_node], vnodes, "Uniform")

        fig, axes = plt.subplots(1, 2, figsize=(10, 4))
        colors = plt.cm.tab10(np.linspace(0, 1, node_count + 1))

        axes[0].bar(dist_before.keys(), dist_before.values(), color=colors[:node_count])
        axes[0].set_title(f"Before — {node_count} nodes")
        axes[0].set_xlabel("Node")
        axes[0].set_ylabel("Keys Owned")

        axes[1].bar(dist_after.keys(), dist_after.values(), color=colors)
        axes[1].set_title(f"After — {node_count + 1} nodes")
        axes[1].set_xlabel("Node")

        plt.tight_layout()
        st.pyplot(fig)

    st.info("Consistent hashing remaps only ~1/N keys when a node is added or removed.")

    # Std-dev table
    st.markdown("#### Load Balance Quality")
    rows = []
    for label, dist in [("Before (add)", dist_before), ("After (add)", dist_after)]:
        vals = list(dist.values())
        rows.append({
            "Scenario":  label,
            "Std Dev":   round(statistics.stdev(vals), 1),
            "Min Load":  min(vals),
            "Max Load":  max(vals),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════
# TAB 3 — Hot-Key Analysis
# ═══════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Hot-Key Skew Detection & Replication Mitigation")
    st.markdown("Shows that consistent hashing alone does **not** prevent hotspots, "
                "and demonstrates mitigation via **key replication + round-robin reads**.")

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        n_requests   = st.slider("Total Requests", 500, 10000, 5000, step=500)
        hot_fraction = st.slider("Hot-Key Fraction", 0.5, 0.99, 0.80, step=0.05)
    with col_s2:
        hot_key  = st.text_input("Hot Key", value="key:hot")
        replicas = st.slider("Replication Factor", 1, min(5, node_count), min(3, node_count))

   # Build workload
    random.seed(42)
    cold_keys = [f"key:{i}" for i in range(200)]

    if workload == "Uniform":
        wl = random.choices(cold_keys, k=n_requests)

    else:
        wl = (
            [hot_key] * int(n_requests * hot_fraction) +
            random.choices(cold_keys, k=int(n_requests * (1 - hot_fraction)))
        )

    random.shuffle(wl)
    hring = build_ring(nodes, vnodes)
    # Without replication
    counts_no_rep = {n: 0 for n in nodes}
    for k in wl:
        counts_no_rep[get_node_from_ring(hring, k)] += 1

    hot_node = get_node_from_ring(hring, hot_key)

    # With replication — round-robin across replica nodes
    def get_replica_nodes(ring, key, n_replicas):
        sorted_keys = sorted(ring.keys())
        h = int(hashlib.md5(key.encode()).hexdigest(), 16)
        start = None
        for i, rk in enumerate(sorted_keys):
            if h <= rk:
                start = i
                break
        if start is None:
            start = 0
        seen, result = set(), []
        for i in range(len(sorted_keys)):
            node = ring[sorted_keys[(start + i) % len(sorted_keys)]]
            if node not in seen:
                seen.add(node)
                result.append(node)
            if len(result) == n_replicas:
                break
        return result

    replica_nodes = get_replica_nodes(hring, hot_key, replicas)
    counts_rep    = {n: 0 for n in nodes}
    rr_counter    = {}
    for k in wl:
        if k == hot_key:
            idx = rr_counter.get(k, 0) % len(replica_nodes)
            counts_rep[replica_nodes[idx]] += 1
            rr_counter[k] = idx + 1
        else:
            counts_rep[get_node_from_ring(hring, k)] += 1

    # ── Metrics row ──────────────────────────────────────────────
    m1, m2, m3 = st.columns(3)
    m1.metric("Hot Node (no replication)", hot_node)
    m2.metric("Hot-Key Replicated To", ", ".join(replica_nodes))
    std_before = round(statistics.stdev(counts_no_rep.values()), 1)
    std_after  = round(statistics.stdev(counts_rep.values()), 1)
    m3.metric("Std Dev (before → after)", f"{std_before} → {std_after}",
              f"{std_after - std_before:+.1f} (lower = better)")

    # ── Side-by-side bar charts ───────────────────────────────────
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    node_labels = list(nodes)
    bar_colors  = ["#e74c3c" if n == hot_node else "#3498db" for n in node_labels]

    axes[0].bar(node_labels,
                [counts_no_rep[n] for n in node_labels],
                color=bar_colors)
    axes[0].set_title("Without Replication")
    axes[0].set_xlabel("Node")
    axes[0].set_ylabel("Requests")

    rep_colors = ["#e67e22" if n in replica_nodes else "#2ecc71" for n in node_labels]
    axes[1].bar(node_labels,
                [counts_rep[n] for n in node_labels],
                color=rep_colors)
    axes[1].set_title(f"With Replication (factor={replicas}, round-robin)")
    axes[1].set_xlabel("Node")

    plt.tight_layout()
    st.pyplot(fig)

    # ── Detail table ─────────────────────────────────────────────
    st.markdown("#### Per-Node Breakdown")
    rows = []
    for n in node_labels:
        rows.append({
            "Node":            n,
            "No Replication":  counts_no_rep[n],
            "With Replication": counts_rep[n],
            "Is Hot Node":     "🔴 Yes" if workload=="Zipfian" and n == hot_node else "—",
            "Is Replica":      "🟠 Yes" if workload=="Zipfian" and n in replica_nodes else "—",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

    if workload == "Zipfian":
        st.success("Replication + round-robin reads distributes hot-key traffic evenly across replica nodes.")
    else:
        st.info("Uniform workload has no dominant hot key, so replication does not significantly change the distribution.")