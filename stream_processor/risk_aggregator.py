kimport json
import time
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer
import numpy as np

# ---------------- CONFIG ----------------
WINDOW_SECONDS = 300
RISK_THRESHOLD = 0.75
COOLDOWN_SECONDS = 120
TREND_WEIGHT = 0.5
# ----------------------------------------

buffers = defaultdict(deque)
last_alert_time = {}

consumer = KafkaConsumer(
    "propagated_stress_linear",  # can switch to centrality for evaluation
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode())
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

for msg in consumer:
    node = msg.value["node_id"]
    stress = msg.value["stress"]
    now = time.time()

    buffers[node].append((now, stress))

    # Remove old values
    while buffers[node] and now - buffers[node][0][0] > WINDOW_SECONDS:
        buffers[node].popleft()

    values = [s for _, s in buffers[node]]
    if len(values) < 3:
        continue

    cumulative_risk = sum(values)

    # Trend via linear slope
    slope = np.polyfit(range(len(values)), values, 1)[0]

    risk_score = cumulative_risk + TREND_WEIGHT * max(0, slope)

    if risk_score >= RISK_THRESHOLD:
        last_time = last_alert_time.get(node, 0)
        if now - last_time >= COOLDOWN_SECONDS:
            alert = {
                "node_id": node,
                "timestamp": now,
                "risk_score": round(risk_score, 3),
                "trend": round(slope, 4),
                "level": "HIGH"
            }
            producer.send("risk_alerts", alert)
            last_alert_time[node] = now

