from kafka import KafkaAdminClient
from kafka.admin import NewTopic


def ensure_topic(topic_name: str, bootstrap_servers: str = "kafka:9092"):
    """Ensure the Kafka topic exists; create it if missing."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        existing = admin.list_topics()
        if topic_name in existing:
            print(f"Kafka topic '{topic_name}' already exists")
            admin.close()
            return True

        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin.create_topics([topic])
        print(f"Created Kafka topic '{topic_name}'")
        admin.close()
        return True
    except Exception as e:
        print(f"Failed to ensure Kafka topic '{topic_name}': {e}")
        return False
