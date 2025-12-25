import json
import random
import networkx as nx

NUM_NODES = 50
EXTRA_EDGE_RATIO = 0.2  # 20% redundancy

nodes = [f"N{i}" for i in range(1, NUM_NODES + 1)]

# Step 1: Build spanning tree (guaranteed connectivity)
G = nx.random_tree(NUM_NODES)
mapping = {i: nodes[i] for i in range(NUM_NODES)}
G = nx.relabel_nodes(G, mapping)

# Step 2: Add redundancy edges
possible_edges = [
    (u, v) for u in nodes for v in nodes
    if u != v and not G.has_edge(u, v)
]

extra_edges = int(len(G.edges) * EXTRA_EDGE_RATIO)
for u, v in random.sample(possible_edges, extra_edges):
    G.add_edge(u, v)

# Step 3: Assign weights
topology = {}
for u, v in G.edges:
    w = round(random.uniform(0.4, 0.9), 2)
    topology.setdefault(u, {})[v] = w
    topology.setdefault(v, {})[u] = w

with open("grid_topology.json", "w") as f:
    json.dump(topology, f, indent=2)

print(f"Generated topology with {NUM_NODES} nodes and {len(G.edges)} edges")

