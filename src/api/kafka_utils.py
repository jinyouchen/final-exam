from kafka import KafkaProducer
import json

class KafkaUtils:
  
    def __init__(self, brokers="localhost:9092,localhost:9094,localhost:9096", topic="sensor-readings"):
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.topic = topic

    def send_reading(self, data):
        self.producer.send(self.topic, value=data)
        self.producer.flush()