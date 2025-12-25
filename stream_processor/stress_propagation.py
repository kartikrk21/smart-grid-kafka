import json
from kafka import KafkaConsumer, KafkaProducer

# ---------------- Load topology ----------------
with open("config/grid_topology.json") as f:
    topology = json.load(f)

# ---------------- Kafka consumers ----------------
consumer = KafkaConsumer(
    "stress_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Stress propagation engine started...")

# ---------------- Main loop ----------------
for msg in consumer:
    event = msg.value

    src = event["node_id"]
    base_stress = event["stress_level"]
    ts = event["timestamp"]

    # Get neighbors safely
    neighbors = topology.get(src, [])

    for neighbor in neighbors:
        # Propagated stress (attenuated)
        propagated_stress = round(base_stress * 0.6, 2)

        # ---- Real-world interpretable metrics ----
        # Voltage drops with stress (simple linear approximation)
        voltage_pu = round(1.0 - 0.1 * propagated_stress, 3)

        # Load increases with stress
        load_pct = round(80 + 20 * propagated_stress, 1)

        propagated_event = {
            "source_node": src,
            "node_id": neighbor,
            "timestamp": ts,
            "stress": propagated_stress,
            "voltage_pu": voltage_pu,
            "load_pct": load_pct
        }

        producer.send("propagated_stress", propagated_event)
        print("Propagated:", propagated_event)

