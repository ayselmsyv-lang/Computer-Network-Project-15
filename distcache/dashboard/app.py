import psutil
import os
import streamlit as st
from visuals import plot_hash_ring, plot_node_distribution
from metrics import distribution_dataframe

st.set_page_config(
    page_title="Distributed Cache Simulator",
    layout="wide"
)

st.markdown("""
<style>
.stApp {
    background-color: #050816;
}
h1, h2, h3 {
    color: white;
}
</style>
""", unsafe_allow_html=True)

st.title("Distributed Cache Infrastructure Simulator")
st.caption("Consistent Hashing • Virtual Nodes • Hot-Key Analysis • Distributed Scaling")

st.sidebar.header("Simulation Settings")
st.sidebar.metric("Runtime Physical CPU Cores", psutil.cpu_count(logical=False))
st.sidebar.metric("Runtime CPU Threads", os.cpu_count())

node_count = st.sidebar.slider("Node Count", 2, 10, 4)
vnodes = st.sidebar.slider("Virtual Nodes", 1, 300, 150)
workload = st.sidebar.selectbox("Workload", ["Uniform", "Zipfian"])

nodes = [f"Node-{i}" for i in range(1, node_count + 1)]

st.subheader("Consistent Hash Ring")
ring_fig = plot_hash_ring(nodes, vnodes)
st.pyplot(ring_fig)

st.subheader("Cluster Overview")

import hashlib

def compute_distribution(nodes, vnodes, workload, n_keys=1000):
    """Real consistent hashing distribution."""
    ring = {}
    for node in nodes:
        for v in range(vnodes):
            h = int(hashlib.md5(f"{node}:{v}".encode()).hexdigest(), 16)
            ring[h] = node
    sorted_keys = sorted(ring.keys())

    def get_node(key):
        h = int(hashlib.md5(key.encode()).hexdigest(), 16)
        for rk in sorted_keys:
            if h <= rk:
                return ring[rk]
        return ring[sorted_keys[0]]

    counts = {n: 0 for n in nodes}

    if workload == "Uniform":
        keys = [f"key:{i}" for i in range(n_keys)]
    else:
        # Zipfian: 80% of requests go to 20% of keys
        import random
        random.seed(42)
        hot_keys = [f"key:{i}" for i in range(n_keys // 5)]
        cold_keys = [f"key:{i}" for i in range(n_keys // 5, n_keys)]
        keys = random.choices(hot_keys, k=int(n_keys * 0.8)) + \
               random.choices(cold_keys, k=int(n_keys * 0.2))

    for key in keys:
        node = get_node(key)
        counts[node] += 1

    return counts

distribution = compute_distribution(nodes, vnodes, workload)

df = distribution_dataframe(distribution)

col1, col2 = st.columns(2)

with col1:
    st.dataframe(df, use_container_width=True)

with col2:
    fig = plot_node_distribution(distribution)
    st.pyplot(fig)

st.divider()

st.subheader("Scaling Analysis")

# Real remapping calculation
import hashlib

def build_ring(nodes, vnodes):
    ring = {}
    for node in nodes:
        for v in range(vnodes):
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

sample_keys = [f"key:{i}" for i in range(1000)]

before_ring = build_ring(nodes, vnodes)
new_node = f"Node-{node_count + 1}"
after_ring = build_ring(nodes + [new_node], vnodes)

remapped = sum(
    1 for k in sample_keys
    if get_node_from_ring(before_ring, k) != get_node_from_ring(after_ring, k)
)
remapped_pct = remapped / len(sample_keys) * 100
expected_pct = 1 / (node_count + 1) * 100

st.metric("Virtual Nodes per Node", vnodes)
st.metric(
    "Keys Rebalanced (if new node added)",
    f"{remapped_pct:.1f}%",
    f"Expected ~{expected_pct:.1f}%"
)

st.info("Consistent hashing minimizes key movement during scaling.")

if workload == "Zipfian":
    hot_node = max(distribution, key=distribution.get)
    hot_pct = distribution[hot_node] / sum(distribution.values()) * 100
    st.error(
        f"Hot-key bottleneck detected on {hot_node} — {hot_pct:.1f}% of all traffic"
    )