import json
from collections import defaultdict, deque
from datetime import datetime

import streamlit as st
import pandas as pd
import networkx as nx
import plotly.graph_objects as go
from kafka import KafkaConsumer

# ================= CONFIG =================
KAFKA_BOOTSTRAP = "localhost:29092"
STRESS_TOPIC = "propagated_stress_linear"
ALERT_TOPIC = "risk_alerts_linear"

POLL_ROUNDS = 10
MAX_TS_POINTS = 120
# =========================================

st.set_page_config(layout="wide")
st.title("⚡ Real-Time Grid Stress Analytics")
st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

# ------------------------------------------------------------------
# INITIALIZE SESSION STATE (CRITICAL FIX)
# ------------------------------------------------------------------
if "latest" not in st.session_state:
    st.session_state.latest = {}

if "stress_ts" not in st.session_state:
    st.session_state.stress_ts = defaultdict(lambda: deque(maxlen=MAX_TS_POINTS))

if "alerts" not in st.session_state:
    st.session_state.alerts = []

# ------------------------------------------------------------------
# DATASET CONTEXT
# ------------------------------------------------------------------
with st.expander("📊 Dataset & System Context", expanded=False):
    st.markdown(
        """
**Data Source**
- Open Power System Data (ENTSO-E)
- Germany (DE) electrical load

**Streaming Model**
- Hourly historical data replayed as real-time
- Kafka-based multi-stage analytics pipeline

**Important Note**
- This is an **event-driven system**
- Metrics update when new events arrive
- Spikes reflect real load events, not noise
        """
    )

# ------------------------------------------------------------------
# LOAD TOPOLOGY
# ------------------------------------------------------------------
with open("grid_topology.json") as f:
    TOPOLOGY = json.load(f)

G = nx.Graph()
for u, nbrs in TOPOLOGY.items():
    for v, w in nbrs.items():
        G.add_edge(u, v, weight=w)

pos = {
    node: (i % 5, -(i // 5))
    for i, node in enumerate(sorted(G.nodes))
}

# ------------------------------------------------------------------
# KAFKA CONSUMERS (LATEST ONLY)
# ------------------------------------------------------------------
stress_consumer = KafkaConsumer(
    STRESS_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

alert_consumer = KafkaConsumer(
    ALERT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ------------------------------------------------------------------
# POLL KAFKA & UPDATE SESSION STATE
# ------------------------------------------------------------------
for _ in range(POLL_ROUNDS):
    records = stress_consumer.poll(timeout_ms=300)
    for msgs in records.values():
        for m in msgs:
            v = m.value
            node = v["node_id"]

            st.session_state.latest[node] = v
            st.session_state.stress_ts[node].append(
                (datetime.now(), v["stress"])
            )

for _ in range(POLL_ROUNDS):
    records = alert_consumer.poll(timeout_ms=300)
    for msgs in records.values():
        for m in msgs:
            st.session_state.alerts.append(m.value)

alerts_df = pd.DataFrame(st.session_state.alerts)

# ------------------------------------------------------------------
# METRICS (NOW PERSISTENT)
# ------------------------------------------------------------------
st.subheader("📈 Live System Metrics")

total_alerts = len(alerts_df)
nodes_alerted = alerts_df["node_id"].nunique() if not alerts_df.empty else 0
avg_risk = alerts_df["risk_score"].mean() if not alerts_df.empty else 0

c1, c2, c3 = st.columns(3)
c1.metric("Total Alerts (session)", total_alerts)
c2.metric("Nodes Affected", nodes_alerted)
c3.metric("Avg Risk", f"{avg_risk:.2f}")

# ------------------------------------------------------------------
# LIVE ALERT TABLE (NO RESET)
# ------------------------------------------------------------------
st.subheader("🚨 Live Risk Alerts")

if not alerts_df.empty:

    def parse_time(t):
        try:
            return pd.to_datetime(t)
        except Exception:
            return None

    alerts_df["time"] = alerts_df["timestamp"].apply(parse_time)

    st.dataframe(
        alerts_df[["node_id", "risk_score", "time"]]
        .sort_values("time", ascending=False)
        .head(20),
        use_container_width=True
    )
else:
    st.info("No alerts yet — waiting for stress accumulation.")

# ------------------------------------------------------------------
# NETWORK GRAPH
# ------------------------------------------------------------------
st.subheader("🕸️ Spatial Stress Propagation")

node_x, node_y, colors, sizes = [], [], [], []

for node in G.nodes:
    x, y = pos[node]
    node_x.append(x)
    node_y.append(y)

    d = st.session_state.latest.get(node, {})
    stress = d.get("stress", 0.0)
    voltage = d.get("voltage_pu", 1.0)

    colors.append(voltage)
    sizes.append(15 + stress * 350)

edge_x, edge_y = [], []
for u, v in G.edges:
    x0, y0 = pos[u]
    x1, y1 = pos[v]
    edge_x.extend([x0, x1, None])
    edge_y.extend([y0, y1, None])

fig_net = go.Figure()

fig_net.add_trace(go.Scatter(
    x=edge_x, y=edge_y,
    mode="lines",
    line=dict(color="gray"),
    hoverinfo="none"
))

fig_net.add_trace(go.Scatter(
    x=node_x, y=node_y,
    mode="markers",
    marker=dict(
        size=sizes,
        color=colors,
        colorscale="RdYlGn",
        cmin=0.9,
        cmax=1.05,
        colorbar=dict(title="Voltage (p.u.)")
    )
))

fig_net.update_layout(
    showlegend=False,
    margin=dict(l=10, r=10, t=10, b=10)
)

st.plotly_chart(fig_net, use_container_width=True)

# ------------------------------------------------------------------
# TIME-SERIES (EVENT-DRIVEN, REALISTIC)
# ------------------------------------------------------------------
st.subheader("⏱️ Stress Evolution (Event-Driven)")

fig_ts = go.Figure()

for node, series in st.session_state.stress_ts.items():
    if len(series) > 3:
        times, values = zip(*series)
        fig_ts.add_trace(go.Scatter(
            x=times, y=values,
            mode="lines",
            name=node
        ))

fig_ts.update_layout(
    xaxis_title="Time",
    yaxis_title="Stress",
    height=350
)

st.plotly_chart(fig_ts, use_container_width=True)

