import json
from confluent_kafka import Producer

KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'data_sync_events'

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_sync_event(payload: dict):
    p = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce(
        TOPIC_NAME, 
        json.dumps(payload).encode('utf-8'), 
        callback=delivery_report
    )

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()
