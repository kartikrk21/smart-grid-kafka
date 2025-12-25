import json
from kafka import KafkaConsumer, KafkaProducer

# Load grid topology
with open("config/grid_topology.json") as f:
    topology = json.load(f)

consumer = KafkaConsumer(
    "fault_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
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
    timestamp = fault["timestamp"]

    # EE-style propagation behavior
    if fault_type == "overvoltage":
        voltage_delta = 0.08
        loading_delta = 0.10
    elif fault_type == "overload":
        voltage_delta = 0.04
        loading_delta = 0.25
    else:  # line_fault
        voltage_delta = 0.12
        loading_delta = 0.30

    neighbors = topology[source_node]

    for neighbor in neighbors:
        propagated_event = {
            "source_node": source_node,
            "affected_node": neighbor,
            "voltage_deviation_pu": round(voltage_delta * severity, 3),
            "loading_increase_pct": round(loading_delta * severity * 100, 1),
            "fault_type": fault_type,
            "timestamp": timestamp
        }

        producer.send("propagated_risks", propagated_event)
        print("Propagated:", propagated_event)

