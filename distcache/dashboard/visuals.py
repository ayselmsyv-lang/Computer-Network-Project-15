import matplotlib.pyplot as plt
import numpy as np
import hashlib

def plot_hash_ring(nodes, vnodes=150):
    fig, ax = plt.subplots(figsize=(6, 6))

    circle = plt.Circle((0, 0), 1, fill=False, color='gray')
    ax.add_artist(circle)

    colors = plt.cm.tab10(np.linspace(0, 1, len(nodes)))

    for i, node in enumerate(nodes):
        # Draw virtual nodes on ring
        for v in range(vnodes):
            h = int(hashlib.md5(f"{node}:{v}".encode()).hexdigest(), 16)
            angle = (h % 1000) / 1000 * 2 * np.pi
            x = np.cos(angle)
            y = np.sin(angle)
            ax.plot(x, y, 'o', color=colors[i], markersize=3, alpha=0.5)

        # Draw physical node label
        angle = i * (2 * np.pi / len(nodes))
        x = np.cos(angle) * 1.25
        y = np.sin(angle) * 1.25
        ax.text(x, y, node, ha='center', va='center', fontsize=9,
                color=colors[i], fontweight='bold')

    ax.set_xlim(-1.6, 1.6)
    ax.set_ylim(-1.6, 1.6)
    ax.axis('off')
    ax.set_title("Consistent Hash Ring (dots = virtual nodes)", fontsize=10)

    return fig

def plot_node_distribution(distribution):
    fig, ax = plt.subplots(figsize=(7, 4))

    nodes = list(distribution.keys())
    values = list(distribution.values())
    colors = plt.cm.tab10(np.linspace(0, 1, len(nodes)))

    ax.bar(nodes, values, color=colors)
    ax.set_title("Node Load Distribution")
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Requests")

    return fig