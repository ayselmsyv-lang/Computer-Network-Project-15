# Project 15: Distributed Cache with Consistent Hashing & Hot-Key Analysis

A high-performance, multi-process simulation of a distributed caching layer. This project implements **Consistent Hashing with Virtual Nodes** to solve the $O(n)$ remapping problem and provides a live cluster environment for analyzing traffic skew and hot-key bottlenecks.

## 🚀 Key Features

* **Consistent Hashing Ring:** Implements a $2^{32}$ hash space to ensure minimal data movement during cluster resizing.
* **Virtual Node Support:** Configurable vnodes (default: 150) to smooth out distribution variance and prevent "data clumping."
* **Multi-Process Isolation:** Spawns independent OS processes for each node, simulating real-world distributed hardware.
* **Workload Analysis:** Contrast engines for **Uniform** traffic vs. **Skewed (Zipfian)** traffic patterns.
* **Dynamic Scaling:** Add or remove nodes at runtime and observe real-time key rebalancing.

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

### 📌 What This Project Demonstrates

This project shows how distributed cache systems reduce key remapping during scaling by using consistent hashing. Instead of redistributing almost all keys when a node is added or removed, only a small portion of keys are moved.

It also demonstrates why virtual nodes are important. Without vnodes, some cache nodes may receive much more traffic than others. With vnodes, key distribution becomes smoother and more balanced.
