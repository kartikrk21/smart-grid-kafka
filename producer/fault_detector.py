import json
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    "sensor_readings",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Fault detector started (data-driven)...")

for msg in consumer:
    data = msg.value
    loading = data["loading_pct"]

    if loading > 85:
        fault = {
            "node": data["node"],
            "fault_type": "overload",
            "severity": round((loading - 85) / 15, 2),
            "timestamp": data["timestamp"]
        }

        producer.send("fault_events", fault)
        print("Detected fault:", fault)

