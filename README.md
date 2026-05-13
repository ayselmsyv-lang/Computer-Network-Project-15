# Project 15: Distributed Cache with Consistent Hashing & Hot-Key Analysis

An educational distributed systems simulator designed to demonstrate scalable cache infrastructure concepts such as consistent hashing, virtual nodes, workload skew, and distributed scaling behavior.

## Live Demo

Here is the link: https://distributed-cache-infrastructure-simulator1.streamlit.app/

# 🚀 Key Features
## Consistent Hash Ring
Implements a circular hash space to minimize key remapping during cluster scaling.

## Virtual Nodes
Supports configurable virtual nodes (vnodes) to improve load balancing and reduce distribution variance.

## Hot-Key Analysis
Simulates Zipfian/skewed workloads where a small subset of keys receives disproportionately high traffic.

## Scaling Simulation
Demonstrates how distributed cache systems rebalance only a small portion of keys when nodes are added or removed.

## Hardware-Aware Simulation
Displays physical CPU cores and logical threads to emulate realistic distributed scaling scenarios.

## Interactive Dashboard
Built with Streamlit for real-time infrastructure visualization and analysis.

---

# Dashboard Preview
<img width="1920" height="998" alt="image" src="https://github.com/user-attachments/assets/65bda766-b014-430b-b152-0e638151b8de" />





## ****The dashboard includes:****

- Cluster Overview
- Scaling Analysis
- Hot-Key Analysis
- Replication Mitigation
- Consistent Hash Ring Visualization
- Virtual Node Distribution
- Node Load Distribution Charts
- Uniform vs Zipfian Workload Comparison
- Hot-Key Bottleneck Detection
- Key Remapping Metrics
- Runtime CPU Hardware Awareness
- Real Consistent Hashing Simulation

---

## Technologies

- Python
- Streamlit
- Pandas
- NumPy
- Matplotlib
- psutil
- Distributed Systems Concepts
- Consistent Hashing
- Virtual Nodes
- Distributed Cache Simulation

## Workload Modes
**Uniform Workload**

Represents relatively balanced traffic across cache nodes.

**Zipfian Workload**

Represents realistic skewed traffic patterns where a small number of keys dominate requests and create hotspots.

---
## 🛠 Usage Reference

The system is controlled via `main.py`. It supports dynamic node counts, even exceeding physical CPU cores through OS context switching.

### Basic Commands
```bash
# Run all demonstrations (Scaling, Hot-Key, and Cluster)
python main.py --demo all

# Launch a live cluster with a specific number of nodes
# Example: 11 nodes on a 10-core machine
python main.py --demo cluster --nodes 11

# Analyze theoretical rebalancing percentages
python main.py --demo scaling
# Analyze hot-key impact under Zipfian workload
python main.py --demo hotkey

# Compare uniform vs skewed traffic distribution
python main.py --demo workload

# Customize virtual node count
python main.py --demo cluster --nodes 5 --vnodes 200
```
### Run Interactive Dashboard

```bash
streamlit run dashboard/app.py
```

### 📌 What This Project Demonstrates

This project shows how distributed cache systems reduce key remapping during scaling by using consistent hashing. Instead of redistributing almost all keys when a node is added or removed, only a small portion of keys are moved.

It also demonstrates why virtual nodes are important. Without vnodes, some cache nodes may receive much more traffic than others. With vnodes, key distribution becomes smoother and more balanced.
