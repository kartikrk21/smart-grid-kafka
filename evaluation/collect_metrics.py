import json
import csv
from kafka import KafkaConsumer

# ================= CONFIG =================
KAFKA_BOOTSTRAP = "localhost:29092"
MAX_MESSAGES = 500
# =========================================

TOPICS = {
    "linear": "risk_alerts_linear",
    "centrality": "risk_alerts_centrality"
}

print("[Evaluation] Collecting alert data from Kafka...")

for label, topic in TOPICS.items():
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    rows = []

    for msg in consumer:
        rows.append(msg.value)
        if len(rows) >= MAX_MESSAGES:
            break

    if not rows:
        print(f"[Warning] No alerts found for {label}")
        continue

    fieldnames = rows[0].keys()
    output_file = f"evaluation/{label}_alerts.csv"

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[Saved] {len(rows)} alerts → {output_file}")

