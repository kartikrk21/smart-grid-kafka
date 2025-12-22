import json
import time
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
from collections import deque

st.set_page_config(page_title="Smart Grid Stress Monitor", layout="wide")

# -------------------- SYSTEM EXPLANATION --------------------
st.title("⚡ Smart Grid Real-Time Stress Monitoring System")

st.markdown("""
### What does this system do?

This system monitors a **power grid in real time** and models how
**electrical stress propagates and accumulates** due to faults.

**Pipeline overview:**
1. Grid sensors stream data continuously
2. Faults occur at individual nodes
3. Stress propagates to neighboring nodes
4. Stress accumulates over time with decay
5. Alerts are generated when stress becomes unsafe

The dashboard visualizes the **final decision layer** of the Kafka streaming system.
""")

# -------------------- KAFKA CONSUMER --------------------
consumer = KafkaConsumer(
    "alerts",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest"
)

# -------------------- STATE --------------------
MAX_EVENTS = 100
alerts_buffer = deque(maxlen=MAX_EVENTS)

# -------------------- LAYOUT --------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("🚨 Live Grid Alerts")

with col2:
    st.subheader("📈 Stress Trend Over Time")

alert_placeholder = col1.empty()
chart_placeholder = col2.empty()

# -------------------- STREAM LOOP --------------------
for message in consumer:
    alert = message.value
    alerts_buffer.append(alert)

    df = pd.DataFrame(alerts_buffer)

    # ---- Alerts ----
    with alert_placeholder.container():
        for a in reversed(list(alerts_buffer)[-5:]):
            if a["level"] == "CRITICAL":
                st.error(f"CRITICAL: Node {a['node']} | Stress {a['stress_index']}")
            else:
                st.warning(f"WARNING: Node {a['node']} | Stress {a['stress_index']}")

    # ---- Graph ----
    with chart_placeholder.container():
        if not df.empty:
            df["time"] = pd.to_datetime(df["timestamp"], unit="s")
            st.line_chart(
                df.groupby("time")["stress_index"].mean()
            )

    time.sleep(0.5)

