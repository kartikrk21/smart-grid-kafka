import json
import time
import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Load cleaned real dataset
df = pd.read_csv("data/opsd_clean.csv")

# Simulate multiple nodes using same real signal
NODES = ["SS_1", "SS_2", "SS_3", "SS_4", "SS_5"]

print("Streaming real power load data into Kafka...")

for _, row in df.iterrows():
    ts = int(pd.to_datetime(row["timestamp"]).timestamp())
    loading = float(row["loading_pct"])

    for node in NODES:
        event = {
            "node": node,
            "timestamp": ts,
            "loading_pct": loading
        }

        producer.send("sensor_readings", event)
        print("Sent:", event)

    time.sleep(0.2)   # controls streaming speed

