import json
from collections import deque, Counter
from datetime import datetime, timedelta

from kafka import KafkaConsumer

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import networkx as nx

# ================= CONFIG =================
KAFKA_BOOTSTRAP = "localhost:29092"

TOPICS = {
    "linear": {
        "stress": "propagated_stress_linear",
        "alerts": "risk_alerts_linear"
    },
    "centrality": {
        "stress": "propagated_stress_centrality",
        "alerts": "risk_alerts_centrality"
    }
}

TOP_K = 5
RECENT_ALERTS = 50
TIMELINE_WINDOW_MIN = 10
# =========================================

# ---------------- Kafka Consumers ----------------
def make_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

stress_consumers = {
    "linear": make_consumer(TOPICS["linear"]["stress"]),
    "centrality": make_consumer(TOPICS["centrality"]["stress"])
}

alert_consumers = {
    "linear": make_consumer(TOPICS["linear"]["alerts"]),
    "centrality": make_consumer(TOPICS["centrality"]["alerts"])
}

# ---------------- In-Memory State ----------------
latest = {"linear": {}, "centrality": {}}
alerts = {"linear": deque(maxlen=500), "centrality": deque(maxlen=500)}
alert_times = {"linear": deque(maxlen=500), "centrality": deque(maxlen=500)}

# ---------------- Grid Topology ----------------
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

# ---------------- Dash App ----------------
app = dash.Dash(__name__)
app.title = "Real-Time Grid Stress Analytics"

# ---------------- Styling ----------------
app.index_string = """
<!DOCTYPE html>
<html>
<head>
<style>
body {
    background-color: #0e1117;
    color: #e6edf3;
    font-family: Arial;
}
.card {
    background: #161b22;
    padding: 16px;
    border-radius: 12px;
    width: 200px;
    text-align: center;
    font-size: 18px;
    white-space: pre-line;
}
.section {
    margin-top: 30px;
}
</style>
</head>
<body>
    {%app_entry%}
    <footer>
        {%config%}
        {%scripts%}
        {%renderer%}
    </footer>
</body>
</html>
"""

# ---------------- Layout ----------------
app.layout = html.Div([
    html.H2("⚡ Real-Time Grid Stress Analytics"),
    html.P("Kafka-based event-driven grid monitoring with comparative propagation models"),

    dcc.Dropdown(
        id="model",
        options=[
            {"label": "Linear Propagation", "value": "linear"},
            {"label": "Centrality-Based Propagation", "value": "centrality"}
        ],
        value="linear",
        clearable=False,
        style={"width": "320px", "marginBottom": "20px"}
    ),

    dcc.Interval(id="tick", interval=1000, n_intervals=0),

    html.Div(id="kpis", style={"display": "flex", "gap": "20px"}),

    html.Div([
        html.H3("🕸️ Grid Stress Propagation"),
        dcc.Graph(id="grid")
    ], className="section"),

    html.Div([
        html.Div([
            html.H4("🚨 Live Alerts"),
            html.Div(id="alerts")
        ], style={"flex": 1}),

        html.Div([
            html.H4("📊 Top-K Critical Nodes (Recent)"),
            dcc.Graph(id="topk")
        ], style={"flex": 1})
    ], style={"display": "flex", "gap": "40px"}, className="section"),

    html.Div([
        html.H4("⏱️ Alert Timeline (Last 10 min)"),
        dcc.Graph(id="timeline")
    ], className="section")
])

# ---------------- Callback ----------------
@app.callback(
    Output("kpis", "children"),
    Output("alerts", "children"),
    Output("grid", "figure"),
    Output("topk", "figure"),
    Output("timeline", "figure"),
    Input("tick", "n_intervals"),
    Input("model", "value")
)
def update(_, model):

    # ---- Consume Kafka ----
    for batch in stress_consumers[model].poll(timeout_ms=100).values():
        for m in batch:
            latest[model][m.value["node_id"]] = m.value

    for batch in alert_consumers[model].poll(timeout_ms=100).values():
        for m in batch:
            alerts[model].appendleft(m.value)
            alert_times[model].append(datetime.now())

    # ---- KPIs ----
    total_alerts = len(alerts[model])
    nodes_alerted = len(set(a["node_id"] for a in alerts[model]))
    avg_risk = round(
        sum(a["risk_score"] for a in alerts[model]) / total_alerts, 2
    ) if total_alerts else 0

    kpis = [
        html.Div(f"🔥 Alerts\n{total_alerts}", className="card"),
        html.Div(f"📍 Nodes\n{nodes_alerted}", className="card"),
        html.Div(f"⚠ Avg Risk\n{avg_risk}", className="card")
    ]

    # ---- Alert Feed ----
    alert_items = [
        html.Div(
            f"{a['node_id']} | risk={a['risk_score']}",
            style={"color": "#ff7b72"}
        )
        for a in list(alerts[model])[:10]
    ]

    # ---- Grid ----
    node_x, node_y, colors, sizes = [], [], [], []
    for n in G.nodes:
        x, y = pos[n]
        node_x.append(x)
        node_y.append(y)

        d = latest[model].get(n, {})
        stress = d.get("stress", 0)
        voltage = d.get("voltage_pu", 1.0)

        colors.append(voltage)
        sizes.append(20 + stress * 300)

    edge_x, edge_y = [], []
    for u, v in G.edges:
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    grid_fig = go.Figure()
    grid_fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        mode="lines",
        line=dict(color="#30363d", width=2),
        hoverinfo="none"
    ))
    grid_fig.add_trace(go.Scatter(
        x=node_x, y=node_y,
        mode="markers+text",
        text=list(G.nodes),
        textposition="bottom center",
        marker=dict(
            size=sizes,
            color=colors,
            colorscale="RdYlGn",
            cmin=0.9,
            cmax=1.05,
            colorbar=dict(title="Voltage (p.u.)")
        )
    ))
    grid_fig.update_layout(
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        font=dict(color="white"),
        showlegend=False
    )

    # ---- Top-K (FIXED SIZE) ----
    recent = list(alerts[model])[:RECENT_ALERTS]
    counts = Counter(a["node_id"] for a in recent)
    top_nodes = counts.most_common(TOP_K)

    x_vals = [n for n, _ in top_nodes]
    y_vals = [c for _, c in top_nodes]
    y_max = max(y_vals) if y_vals else 1

    topk_fig = go.Figure(go.Bar(x=x_vals, y=y_vals, marker_color="#ff7b72"))
    topk_fig.update_layout(
        autosize=False,
        height=300,
        margin=dict(l=40, r=20, t=40, b=40),
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        font=dict(color="white"),
        yaxis=dict(range=[0, y_max + 1], fixedrange=True)
    )

    # ---- Timeline ----
    cutoff = datetime.now() - timedelta(minutes=TIMELINE_WINDOW_MIN)
    recent_times = [t for t in alert_times[model] if t >= cutoff]
    timeline_counts = Counter(t.replace(second=0, microsecond=0) for t in recent_times)

    times = sorted(timeline_counts.keys())
    vals = [timeline_counts[t] for t in times]

    timeline_fig = go.Figure(go.Bar(x=times, y=vals, marker_color="#58a6ff"))
    timeline_fig.update_layout(
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        font=dict(color="white"),
        height=250
    )

    return kpis, alert_items, grid_fig, topk_fig, timeline_fig


# ---------------- Run ----------------
if __name__ == "__main__":
    app.run(debug=False)

