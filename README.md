# Real-Time UPI Fraud Detection & Alerting System

A **real-time, fintech-grade fraud detection system** inspired by Indian UPI transaction flows.  
Built locally using **Apache Kafka, Redis, Python, and classical Machine Learning**.

This project demonstrates how **rule-based fraud detection and ML-based risk scoring** work together in production banking systems.

---

## 🔍 Problem Statement

UPI fraud in India often involves:
- Rapid transaction bursts
- First-time high-value payments
- New beneficiary scams
- Mule accounts receiving money from many users
- Late-night social-engineering attacks

Traditional rule-only systems cause false positives.  
ML-only systems lack explainability.

👉 **This project uses a hybrid approach**.

---

## 🧠 Solution Overview

The system:
- Streams UPI-like transactions via Kafka
- Tracks short-term user behavior in Redis
- Applies explainable fraud rules
- Uses a trained ML model to estimate fraud probability
- Combines both into a final risk score
- Emits real-time fraud alerts without blocking transactions

---

## 🏗️ Architecture

Producer (UPI Simulator)
        ↓
   Kafka Topic
        ↓
Fraud Consumer
  ├── Redis (behavior state)
  ├── Rule Engine
  ├── ML Inference
        ↓
 Fraud Alerts (stdout / logs)


---

## ⚙️ Tech Stack

- **Language:** Python 3
- **Streaming:** Apache Kafka
- **State Store:** Redis
- **ML:** scikit-learn (Logistic Regression)
- **Kafka Client:** confluent-kafka
- **Environment:** Local laptop (WSL / Linux / macOS)

---

## 🇮🇳 Fraud Rules Implemented

- High transaction velocity (burst detection)
- High total amount within short window
- New beneficiary risk
- Late-night transactions
- Mule / fan-in account patterns

Rules are **fully explainable** and configurable via YAML.

---

## 🤖 Machine Learning Design

### Features
- Transaction amount
- Transaction count (5 min)
- Total amount (5 min)
- Unique payees (5 min)
- New beneficiary flag
- Hour of day
- Rule-based risk score

### Model
- Logistic Regression
- Class-imbalance aware
- Fully explainable coefficients

### Labeling Strategy
- Offline **weak supervision**
- Fraud-like behavior is **seeded only during training**
- Production scoring logic remains unchanged

---

## 🚀 How to Run Locally

### 1️⃣ Start Kafka
```bash
bin/kafka-server-start.sh config/server.properties

### 2️⃣ Start Redis
```bash
redis-server

### 3️⃣ Activate Python Environment
```bash
source .venv/bin/activate

### 4️⃣ Start Fraud Consumer
```bash
python -m consumer.fraud_pipeline

### 5️⃣ Start UPI Transaction Simulator
```bash
python3 producer/upi_simulator.py

### Sample Output
```bash
[EVENT] payer=user42@upi amount=38000 rule_score=60 ml_prob=0.81 final_risk=72 rules=['HIGH_AMOUNT_BURST']
🚨 FRAUD ALERT | payer=user42@upi risk=72 rules=['HIGH_AMOUNT_BURST']

### Offline ML Training
```bash
python -m ml.generate_dataset
python ml/train.py


### Generated Artifacts
ml/model.pkl
ml/scaler.pkl


---
