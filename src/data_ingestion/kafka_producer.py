import json
import os
from datetime import datetime
from kafka import KafkaProducer
from configs import KAFKA_BROKER

class KafkaDataProducer:
    """Generic Kafka Producer to send data to a Kafka topic."""
    
    def __init__(self, topic, broker=KAFKA_BROKER):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send_data(self, data):
        """Send JSON data to Kafka topic."""
        try:
            self.producer.send(self.topic, value=data)
            self.producer.flush()
            print(f"Data sent to Kafka topic '{self.topic}'- {datetime.now()}")
        except Exception as e:
            print(f"Failed to send data to Kafka: {e}")

    def close(self):
        """Close the Kafka producer."""
        self.producer.close()