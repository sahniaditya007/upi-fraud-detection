import json
from datetime import datetime
from confluent_kafka import Producer


ALERT_TOPIC = "upi.fraud.alerts"
BOOTSTRAP_SERVERS = "localhost:9092"


producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "linger.ms": 5,
}


producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err:
        print(f"[ALERT-SINK] Delivery failed: {err}")
    else:
        print(
            f"[ALERT-SINK] Alert delivered to {msg.topic()} "
            f"[partition={msg.partition()}, offset={msg.offset()}]"
        )


def publish_alert(transaction: dict, risk_score: int, triggered_rules: list):
    """
    Publish fraud alert to Kafka
    """

    alert_payload = {
        "alert_id": f"ALERT-{transaction['txn_id']}",
        "txn_id": transaction["txn_id"],
        "user_id": transaction["user_id"],
        "amount": transaction["amount"],
        "risk_score": risk_score,
        "rules_triggered": triggered_rules,
        "timestamp": datetime.utcnow().isoformat(),
    }

    producer.produce(
        topic=ALERT_TOPIC,
        key=str(transaction["user_id"]),
        value=json.dumps(alert_payload),
        on_delivery=delivery_report,
    )

    producer.poll(0)


def flush():
    producer.flush()