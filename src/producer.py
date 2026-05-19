import argparse
import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC = "sensor-events"

SENSOR_CONFIGS = {
    "temperature": {"min": 15.0, "max": 45.0, "unit": "°C"},
    "humidity": {"min": 30.0, "max": 95.0, "unit": "%"},
    "pressure": {"min": 980.0, "max": 1040.0, "unit": "hPa"}
}

def generate_sensor_value(sensor_type):
    cfg = SENSOR_CONFIGS[sensor_type]
    if random.random() < 0.1:
        return round(random.choice([cfg["min"] - 10, cfg["max"] + 10]), 2)
    return round(random.uniform(cfg["min"], cfg["max"]), 2)

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            acks="all",
            retries=3,
            linger_ms=5,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 8, 0),
            request_timeout_ms=30000,
            connections_max_idle_ms=540000
        )
        return producer
    except KafkaError as e:
        print(f"Failed to connect to Kafka: {str(e)}")
        raise SystemExit(1)

def main():
    parser = argparse.ArgumentParser(description="Kafka Sensor Data Producer")
    parser.add_argument("--count", type=int, default=100, help="Total number of events to send")
    parser.add_argument("--rate", type=int, default=10, help="Events per second")
    parser.add_argument("--source", type=str, default="site-A-rack-12", help="Sensor source identifier")
    args = parser.parse_args()

    producer = create_kafka_producer()
    sensor_types = list(SENSOR_CONFIGS.keys())
    print(f"Starting to send {args.count} messages to topic [{TOPIC}] at rate: {args.rate} messages/second")

    for i in range(args.count):
        sensor_type = random.choice(sensor_types)
        value = generate_sensor_value(sensor_type)
        is_anomaly = not (SENSOR_CONFIGS[sensor_type]["min"] <= value <= SENSOR_CONFIGS[sensor_type]["max"])
        
        message = {
            "sensor_type": sensor_type,
            "value": value,
            "unit": SENSOR_CONFIGS[sensor_type]["unit"],
            "timestamp": int(time.time() * 1000),
            "source": args.source,
            "is_anomaly": is_anomaly
        }

        try:
            future = producer.send(TOPIC, key=sensor_type, value=message)
            record_metadata = future.get(timeout=10)
            print(f"[{i+1}/{args.count}] Sent successfully | Partition:{record_metadata.partition} | Offset:{record_metadata.offset} | Message:{message}")
        except KafkaError as e:
            print(f"[{i+1}/{args.count}] Failed to send: {str(e)} | Message:{message}")

        time.sleep(1 / args.rate)

    producer.flush()
    producer.close()
    print(f"\nSending completed! Total {args.count} messages sent to Kafka topic [{TOPIC}]")

if __name__ == "__main__":
    main()