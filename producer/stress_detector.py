import json
import random
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = "localhost:29092"

LOAD_THRESHOLD = 0.72
MAX_LOAD = 80000

SOURCE_NODES = {
    "N1": 1.0,   # major substation
    "N7": 0.85,  # medium
    "N12": 0.75  # smaller
}

consumer = KafkaConsumer(
    "grid_measurements",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("[Stress Detector] Running")

for msg in consumer:
    record = msg.value
    load = record["load"]
    ts = record["timestamp"]

    normalized = load / MAX_LOAD
    base = max(0.0, normalized - LOAD_THRESHOLD)

    for node, capacity in SOURCE_NODES.items():
        stress = base * capacity * random.uniform(1.2, 1.8)
        stress = min(stress, 0.5)

        if stress > 0:
            producer.send(
                "stress_events",
                {
                    "node_id": node,
                    "stress_level": round(stress, 3),
                    "timestamp": ts
                }
            )

