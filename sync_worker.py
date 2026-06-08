import json
import time
from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'data_sync_events'
GROUP_ID = 'ai_bi_sync_group'

def process_event(event_data):
    """
    This function handles the logic of taking data from rivus-bi
    and applying it to rivus-ai (or vice-versa).
    """
    print(f"Processing event from {event_data.get('source')} to {event_data.get('target')}")
    print(f"Entity ID: {event_data.get('entity_id')}")
    print(f"Data: {event_data.get('data')}")
    
    # Here you would typically make a REST API call or database update
    # to the target system (e.g., rivus-ai/Data Formulator).
    # For now, we simulate a small delay to represent network/processing time.
    time.sleep(1)
    print("Event processed successfully.\n")

def main():
    c = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

    c.subscribe([TOPIC_NAME])
    print(f"Subscribed to topic: {TOPIC_NAME}")

    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            try:
                event_data = json.loads(msg.value().decode('utf-8'))
                process_event(event_data)
            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == '__main__':
    # Wait a bit for Kafka to be fully ready before starting the consumer
    time.sleep(10)
    main()
