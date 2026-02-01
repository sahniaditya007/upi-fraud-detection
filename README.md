# Real-Time UPI Fraud Detection System

A local, laptop-based real-time fraud detection system inspired by Indian UPI transaction flows.

## Tech Stack
- Python
- Apache Kafka (KRaft mode)
- Redis
- scikit-learn

## Core Ideas
- Event-driven architecture using Kafka
- Short-term behavioral state in Redis
- Hybrid fraud detection:
  - Rule-based risk scoring (explainable)
  - Classical ML probability scoring
- Non-blocking, real-time alerting

## Project Phases
1. Kafka streaming & ingestion
2. Redis behavior tracking
3. Rule-based fraud detection
4. ML model training & scoring
5. Alerting & evaluation

## Constraints
- Runs entirely on a local machine
- No cloud services
- No managed Kafka
