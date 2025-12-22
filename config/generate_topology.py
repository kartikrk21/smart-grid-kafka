import json

nodes = [f"N{i}" for i in range(1, 51)]
topology = {}

for i, node in enumerate(nodes):
    neighbors = []
    if i > 0:
        neighbors.append(nodes[i-1])
    if i < len(nodes)-1:
        neighbors.append(nodes[i+1])
    topology[node] = neighbors

with open("grid_topology.json", "w") as f:
    json.dump(topology, f, indent=2)

print("Generated grid with", len(nodes), "nodes")

