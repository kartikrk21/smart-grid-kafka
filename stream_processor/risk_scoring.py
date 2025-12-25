import json
import time
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict, deque

# =========================
# Kafka setup
# =========================)
consumer = KafkaConsumer(
    "propagated_risks",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# =========================
# Window + State Store Config
# =========================
WINDOW_SEC = 300   # 5-minute sliding window

# Per-node state stores
voltage_state = defaultdict(deque)   # node -> [(timestamp, voltage_dev)]
loading_state = defaultdict(deque)   # node -> [(timestamp, loading_pct)]

# Alert thresholds (EE-friendly)
WARNING_THRESHOLD = 0.4
CRITICAL_THRESHOLD = 0.7

print("Outage risk aggregation engine started...")

# =========================
# Helper function
# =========================
def cleanup_old_entries(state_queue, current_time):
    """
    Remove entries older than the window size
    """
    while state_queue and current_time - state_queue[0][0] > WINDOW_SEC:
        state_queue.popleft()

# =========================
# Main consumer loop
# =========================
for message in consumer:
    event = message.value

    node = event["affected_node"]
    voltage_dev = event["voltage_deviation_pu"]       # pu deviation
    loading_inc = event["loading_increase_pct"]        # percentage
    current_time = event["timestamp"]

    # -------------------------
    # Update state stores
    # -------------------------
    voltage_state[node].append((current_time, voltage_dev))
    loading_state[node].append((current_time, loading_inc))

    # Cleanup old window data
    cleanup_old_entries(voltage_state[node], current_time)
    cleanup_old_entries(loading_state[node], current_time)

    # -------------------------
    # Compute windowed averages
    # -------------------------
    avg_voltage_dev = sum(v for _, v in voltage_state[node]) / len(voltage_state[node])
    avg_loading = sum(v for _, v in loading_state[node]) / len(loading_state[node])

    # -------------------------
    # Outage risk model (simple but realistic)
    # -------------------------
    outage_risk = min(
        1.0,
        (avg_voltage_dev * 2.0) + (avg_loading / 150.0)
    )

    # -------------------------
    # Alert logic
    # -------------------------
    level = None
    if outage_risk >= CRITICAL_THRESHOLD:
        level = "CRITICAL"
    elif outage_risk >= WARNING_THRESHOLD:
        level = "WARNING"

    if level:
        alert = {
            "node": node,
            "outage_risk": round(outage_risk, 2),
            "avg_voltage_dev_pu": round(avg_voltage_dev, 3),
            "avg_loading_pct": round(avg_loading, 1),
            "level": level,
            "timestamp": current_time
        }

        producer.send("alerts", alert)
        print("ALERT:", alert)

