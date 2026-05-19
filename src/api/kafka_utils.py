from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json

class KafkaUtils:
    def __init__(self, brokers="localhost:9092,localhost:9094,localhost:9096"):
        self.brokers = brokers
        self.admin_client = KafkaAdminClient(bootstrap_servers=brokers)

    def create_topic(self, topic_name, partitions=3, replication_factor=3):
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=partitions,
                replication_factor=replication_factor
            )
            self.admin_client.create_topics([topic])
            print(f" Topic created: {topic_name}")
        except TopicAlreadyExistsError:
            print(f" Topic already exists: {topic_name}")

    def get_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.brokers.split(","),
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3
        )

    def get_consumer(self, topic, group_id="sensor-group"):
        return KafkaConsumer(
            topic,
            bootstrap_servers=self.brokers.split(","),
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

    def list_topics(self):
        return self.admin_client.list_topics()