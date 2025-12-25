import json
import random
from collections import deque
from kafka import KafkaConsumer, KafkaProducer

# ================= CONFIG =================
KAFKA_BOOTSTRAP = "localhost:29092"   # MUST match docker-compose
MAX_DEPTH = 3                         # BFS depth
DEPTH_DECAY = 0.7                     # decay per hop
# =========================================

# -------- Load Grid Topology --------
with open("grid_topology.json") as f:
    TOPOLOGY = json.load(f)

# Node sensitivity (heterogeneity)
node_sensitivity = {
    node: random.uniform(0.8, 1.2) for node in TOPOLOGY
}

# -------- Kafka Consumer / Producer --------
consumer = KafkaConsumer(
    "stress_events",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("[Linear Propagation] Started")

# -------- BFS Stress Propagation --------
for msg in consumer:
    event = msg.value

    source = event.get("node_id")
    base_stress = event.get("stress_level")
    ts = event.get("timestamp")

    if source is None or base_stress is None or ts is None:
        continue

    visited = set([source])
    queue = deque([(source, base_stress, 0)])

    while queue:
        current, stress, depth = queue.popleft()

        if depth >= MAX_DEPTH:
            continue

        for neighbor, weight in TOPOLOGY[current].items():
            if neighbor in visited:
                continue

            visited.add(neighbor)

            propagated = (
                stress
                * weight
                * node_sensitivity[neighbor]
                * (DEPTH_DECAY ** depth)
            )

            output = {
                "source_node": source,
                "node_id": neighbor,
                "hop": depth + 1,
                "timestamp": ts,
                "stress": round(propagated, 4),
                "voltage_pu": round(1.0 - propagated * 0.1, 3),
                "load_pct": round(70 + propagated * 40, 1)
            }

            producer.send("propagated_stress_linear", output)
            queue.append((neighbor, propagated, depth + 1))

