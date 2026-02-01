import json

from confluent_kafka import Consumer, KafkaException, KafkaError

from consumer.redis_state import update_redis_state, get_redis_state
from rules.rule_engine import RuleEngine
from ml.features import extract_features
from ml.inference import predict_proba


# -----------------------------
# Kafka configuration
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "upi.transactions.raw"
CONSUMER_GROUP = "fraud-detector-v1"

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

consumer = Consumer(consumer_conf)

rule_engine = RuleEngine()


# -----------------------------
# Event processing
# -----------------------------
def process_event(event, metadata):
    """
    Full hybrid fraud evaluation:
    Redis → Rules → ML → Final Risk
    """

    # 1️⃣ Update Redis state
    update_redis_state(event)

    # 2️⃣ Fetch Redis-derived features
    redis_state = get_redis_state(event)
    
    if redis_state is None:
        redis_state = {
            "txn_count_5m": 0,
            "total_amount_5m": 0,
            "unique_payees_5m": 0,
            "is_new_beneficiary": True,
            "unique_senders": 0,
        }

    # 3️⃣ Rule-based scoring
    rule_score, triggered_rules = rule_engine.evaluate(event, redis_state)

    # 4️⃣ ML inference
    features = extract_features(event, redis_state, rule_score)
    ml_prob = predict_proba(features)

    # 5️⃣ Hybrid risk score (rule-dominant)
    final_risk = int(0.6 * rule_score + 0.4 * (ml_prob * 100))

    # 6️⃣ Logging
    print(
        f"[EVENT] payer={event['payer_upi_id']} "
        f"amount={event['amount']} "
        f"rule_score={rule_score} "
        f"ml_prob={ml_prob:.2f} "
        f"final_risk={final_risk} "
        f"rules={triggered_rules}"
    )

    # 7️⃣ Non-blocking alert
    if final_risk >= 70:
        print(
            f"🚨 FRAUD ALERT | payer={event['payer_upi_id']} "
            f"risk={final_risk} rules={triggered_rules}"
        )


# -----------------------------
# Main consumer loop
# -----------------------------
def run():
    print("Starting fraud pipeline consumer...")
    consumer.subscribe([TOPIC_NAME])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            event = json.loads(msg.value().decode("utf-8"))

            metadata = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp(),
            }

            process_event(event, metadata)

    except KeyboardInterrupt:
        print("\nStopping Consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    run()
