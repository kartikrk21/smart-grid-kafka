# Smart Grid Fault Propagation using Kafka Streaming

## Overview
This project implements a real-time fault propagation and stress monitoring
system for a smart power grid using Apache Kafka.

Unlike traditional systems that detect faults in isolation, this system
models cascading electrical stress across connected grid components
and triggers alerts based on cumulative stress over time.

## System Architecture
Fault Injector → Kafka → Propagation Engine → Stress Aggregator → Alerts → Dashboard

## Grid Model
- Each node represents a power substation or distribution cluster
- Grid connectivity is defined via an external topology configuration
- The system scales to tens or hundreds of nodes without code changes

## Key Features
- Topology-aware fault propagation
- Fault-type dependent stress coupling
- Stateful stress accumulation with decay
- Multi-level alerts (WARNING / CRITICAL)
- Real-time visualization using Streamlit

## Tech Stack
- Apache Kafka (Docker)
- Python
- kafka-python
- Streamlit
- Pandas

## How to Run
1. Start Kafka:
   ```bash
   cd docker
   docker compose up -d

Run components in separate terminals:

python3 producer/fault_injector.py
python3 stream_processor/propagation_engine.py
python3 stream_processor/risk_scoring.py
python3 -m streamlit run dashboard/app.py
