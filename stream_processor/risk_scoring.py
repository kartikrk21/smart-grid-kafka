import json
import time
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict

consumer = KafkaConsumer(
    "propagated_risks",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Cumulative electrical stress per node
stress_state = defaultdict(float)

# Realistic thresholds
WARNING_THRESHOLD = 0.6
CRITICAL_THRESHOLD = 1.0

# Stress decay (cooling / recovery)
DECAY_FACTOR = 0.9

print("Stress aggregation & alert engine started...")

for message in consumer:
    event = message.value
    node = event["affected_node"]
    stress_increment = event["stress_increment"]

    # decay old stress
    stress_state[node] *= DECAY_FACTOR
    stress_state[node] += stress_increment

    level = None
    if stress_state[node] >= CRITICAL_THRESHOLD:
        level = "CRITICAL"
    elif stress_state[node] >= WARNING_THRESHOLD:
        level = "WARNING"

    if level:
        alert = {
            "node": node,
            "stress_index": round(stress_state[node], 2),
            "level": level,
            "timestamp": int(time.time())
        }

        producer.send("alerts", alert)
        print("ALERT:", alert)

