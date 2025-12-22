import json
from kafka import KafkaConsumer, KafkaProducer

# Load grid topology
with open("config/grid_topology.json") as f:
    topology = json.load(f)

consumer = KafkaConsumer(
    "fault_events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Fault propagation engine started...")

for message in consumer:
    fault = message.value
    source_node = fault["node"]
    severity = fault["severity"]
    fault_type = fault["fault_type"]

    # Dynamic propagation factor (physical intuition)
    if fault_type == "overvoltage":
        factor = 0.7   # voltage surges couple strongly
    else:
        factor = 0.5   # current overloads dissipate faster

    neighbors = topology.get(source_node, [])

    for neighbor in neighbors:
        propagated_event = {
            "source_node": source_node,
            "affected_node": neighbor,
            # propagated electrical stress
            "stress_increment": round(severity * factor, 2),
            "fault_type": fault_type,
            "timestamp": fault["timestamp"]
        }

        producer.send("propagated_risks", propagated_event)
        print("Propagated:", propagated_event)

