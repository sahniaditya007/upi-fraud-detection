import json
import time
import uuid
import random
from datetime import datetime

from confluent_kafka import Producer


# -----------------------------
# Kafka configuration
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "upi.transactions.raw"

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "linger.ms": 5,
}

producer = Producer(producer_conf)


# -----------------------------
# Static reference data
# -----------------------------
BANKS = ["HDFC", "ICICI", "SBI", "AXIS", "KOTAK"]
APPS = ["GPay", "PhonePe", "Paytm"]
GEO_HASHES = ["u4pruy", "u4pruz", "u4pruv", "u4xj7n"]

PAYEE_TYPES = ["individual", "merchant"]
TXN_TYPES = ["P2P", "P2M"]


# -----------------------------
# Synthetic user universe
# -----------------------------
NUM_USERS = 300
NUM_MERCHANTS = 50

USERS = [
    {
        "upi_id": f"user{i}@upi",
        "bank": random.choice(BANKS),
        "device_id": f"device-{i}",
        "geo": random.choice(GEO_HASHES),
    }
    for i in range(NUM_USERS)
]

MERCHANTS = [
    {
        "upi_id": f"merchant{i}@upi",
        "bank": random.choice(BANKS),
    }
    for i in range(NUM_MERCHANTS)
]


# -----------------------------
# Helpers
# -----------------------------
def generate_amount():
    r = random.random()
    if r < 0.7:
        return random.randint(1, 2000)
    elif r < 0.95:
        return random.randint(2000, 10000)
    else:
        return random.randint(10000, 50000)


def delivery_report(err, msg):
    if err:
        print(f"[DELIVERY ERROR] {err}")
    else:
        print(
            f"[DELIVERED] topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def generate_event():
    payer = random.choice(USERS)

    is_merchant = random.random() < 0.3
    if is_merchant:
        payee = random.choice(MERCHANTS)
        payee_type = "merchant"
        txn_type = "P2M"
    else:
        payee = random.choice(USERS)
        payee_type = "individual"
        txn_type = "P2P"

    event_time = int(time.time() * 1000)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": event_time,
        "ingest_time": int(time.time() * 1000),

        "payer_upi_id": payer["upi_id"],
        "payer_bank": payer["bank"],
        "payer_device_id": payer["device_id"],
        "payer_geo_hash": payer["geo"],

        "payee_upi_id": payee["upi_id"],
        "payee_bank": payee["bank"],
        "payee_type": payee_type,

        "amount": generate_amount(),
        "currency": "INR",
        "txn_type": txn_type,
        "app_name": random.choice(APPS),
    }

    return event


# -----------------------------
# Main loop (bursty, continuous)
# -----------------------------
def run():
    print("Starting UPI transaction simulator...")
    print("Producing to topic:", TOPIC_NAME)

    while True:
        idle_time = random.uniform(3, 15)
        time.sleep(idle_time)

        burst_size = random.randint(20, 120)
        print(f"\n[BURST] Generating {burst_size} transactions")

        for _ in range(burst_size):
            event = generate_event()

            producer.produce(
                topic=TOPIC_NAME,
                key=event["payer_upi_id"],
                value=json.dumps(event),
                on_delivery=delivery_report,
            )

            producer.poll(0)
            time.sleep(random.uniform(0.005, 0.03))


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
