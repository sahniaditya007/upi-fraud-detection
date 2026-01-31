import json
import time
from datetime import datetime
from consumer.redis_state import update_redis_state
from confluent_kafka import Consumer, KafkaException, KafkaError

# -----------------------------
# Kafka configuration
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "upi.transaction.raw"
CONSUMER_GROUP = "fraud-detector-v1"

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

consumer = Consumer(consumer_conf)

# -----------------------------
# Processing stub
# -----------------------------
def process_event(event, metadata):
    update_redis_state(event)
    
    print(
        f"[EVENT] payer={event['payer_upi_id']} "
        f"payee={event['payee_upi_id']} "
        f"amount={event['amount']} "
        f"partition={metadata['partition']} "
        f"offset={metadata['offset']}"
    )
    
#Main
def run():
    print("Starting fraud pipeline consumer...")
    consumer.subscribe([TOPIC_NAME])
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PATITION_EOF:
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
        print("\n Stopping Consumer...")
    
    finally:
        consumer.close()

if __name__ == "__main__":
    run()