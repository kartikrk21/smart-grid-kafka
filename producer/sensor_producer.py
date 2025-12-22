import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

nodes = ["A", "B", "C", "D"]

while True:
    event = {
        "node": random.choice(nodes),
        "voltage": round(random.uniform(220, 260), 2),
        "current": round(random.uniform(5, 20), 2),
        "timestamp": int(time.time())
    }

    producer.send("sensor_readings", event)
    print("Sent:", event)

    time.sleep(1)

