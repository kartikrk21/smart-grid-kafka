# ⚡ Kafka-Based Real-Time Grid Stress Analytics

A real-time, Kafka-driven analytics system that simulates electrical grid stress,
propagates it across a network topology, and generates risk alerts with live visualization.

This project focuses on **stream processing, system design, and real-time analytics** —
not power-flow simulation or machine learning.

---

## 🔍 Project Overview

The system replays historical power demand data as a live stream and processes it through
multiple Kafka stages:

1. Load ingestion
2. Stress detection
3. Spatial stress propagation
4. Time-windowed risk aggregation
5. Real-time dashboard visualization

Two propagation models are implemented and compared:
- **Linear attenuation**
- **Centrality-based propagation**

---

## 🏗️ Architecture

Historical Load Data
↓
Kafka Producer
↓
grid_measurements
↓
stress_events
↓
propagated_stress_(linear / centrality)
↓
risk_alerts_(linear / centrality)
↓
Real-Time Dashboard (Dash)


Each stage is decoupled using Kafka topics.

---

## 📊 Dataset

**Source:** Open Power System Data (ENTSO-E)  
**Region:** Germany (DE)  
**Type:** Hourly electrical load time series  

The raw dataset is **not included in this repository** due to GitHub size limits.
Only processed or sampled data is used during runtime.

---

## 🖥️ Dashboard Features

- Live grid topology with stress visualization
- KPI cards (alerts, affected nodes, average risk)
- Top-K critical nodes (sliding window)
- Alert timeline (last 10 minutes)
- Toggle between propagation models

---

## 🚀 How to Run

### 🔧 Prerequisites (Both Windows & macOS)

- Python 3.9+
- Apache Kafka + Zookeeper (or Docker)
- Git

Install Python dependencies:

```bash
pip install -r requirements.txt


▶️ Start Kafka
Option A: Using Docker (Recommended)
docker-compose up

Option B: Local Kafka

Ensure Kafka is running on:

localhost:29092

▶️ Run the Pipeline (Order Matters)
1. Start Producer
python producer/grid_data_streamer.py

2. Start Stress Detection
python producer/stress_detector.py

3. Start Propagation (choose one or both)
python stream_processor/stress_propagation_linear.py
python stream_processor/stress_propagation_centrality.py

4. Start Risk Aggregation
python stream_processor/risk_aggregator_linear.py
python stream_processor/risk_aggregator_centrality.py

▶️ Run Dashboard
python realtime_ui/app.py


Open in browser:

http://127.0.0.1:8050


Use the dropdown to switch between propagation models.

🧠 Key Design Choices

Kafka for decoupled, scalable stream processing

Sliding windows to avoid cumulative distortion

Fixed-size real-time UI to prevent layout drift

Comparative evaluation via topic isolation

📌 Notes

This project is intended for academic / demonstration purposes

No deep electrical modeling is performed

Emphasis is on real-time systems and analytics
