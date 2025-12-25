import json
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = "localhost:29092"

WINDOW_EVENTS = 10
RISK_THRESHOLD = 0.7
COOLDOWN_EVENTS = 12

consumer = KafkaConsumer(
    "propagated_stress_centrality",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

buffers = defaultdict(lambda: deque(maxlen=WINDOW_EVENTS))
cooldowns = defaultdict(int)

print("[Centrality Risk Aggregator] Running")

for msg in consumer:
    v = msg.value
    node = v["node_id"]
    stress = v["stress"]

    buffers[node].append(stress)

    if cooldowns[node] > 0:
        cooldowns[node] -= 1
        continue

    risk = sum(buffers[node])

    if risk >= RISK_THRESHOLD:
        producer.send(
            "risk_alerts_centrality",
            {
                "node_id": node,
                "timestamp": v["timestamp"],
                "risk_score": round(risk, 3),
                "level": "HIGH"
            }
        )
        cooldowns[node] = COOLDOWN_EVENTS

