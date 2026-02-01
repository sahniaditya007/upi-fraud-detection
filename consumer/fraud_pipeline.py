import json
from datetime import datetime

from confluent_kafka import Consumer, KafkaException, KafkaError

from consumer.redis_state import update_redis_state, get_redis_state
from rules.rule_engine import RuleEngine


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
    update_redis_state(event)

    risk_score, triggered_rules = evaluate_rules(event)

    print(
        f"[EVENT] payer={event['payer_upi_id']} "
        f"amount={event['amount']} "
        f"risk={risk_score} "
        f"rules={triggered_rules}"
    )

    if risk_score >= 70:
        print("FRAUD ALERT")



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
