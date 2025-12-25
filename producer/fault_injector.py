import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open("config/grid_topology.json") as f:
    topology = json.load(f)

nodes = list(topology.keys())

fault_types = [
    "overvoltage",
    "overload",
    "line_fault"
]
print("Fault injector started...")

while True:
    fault = {
        "node": random.choice(nodes),
        "fault_type": random.choice(fault_types),
        # normalized disturbance magnitude (0–1)
        "severity": round(random.uniform(0.7, 1.0), 2),
        "timestamp": int(time.time())
    }

    producer.send("fault_events", fault)
    print("Injected fault:", fault)

    time.sleep(6)

