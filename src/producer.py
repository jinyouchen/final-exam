import argparse
import json
import time
import random
from kafka import KafkaProducer

RANGES = {
    "temperature": (15.0, 45.0),
    "humidity": (30.0, 95.0),
    "pressure": (980.0, 1040.0)
}
ANOMALY_THRESHOLD = 0.1

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=float, default=5.0)
    parser.add_argument("--source", type=str, default="site-A-rack-12")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092", "localhost:9094", "localhost:9096"],
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=10,
        batch_size=32768,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    sensors = ["temperature", "humidity", "pressure"]
    interval = 1.0 / args.rate

    for _ in range(args.count):
        sensor = random.choice(sensors)
        min_val, max_val = RANGES[sensor]
        is_anomaly = random.random() < ANOMALY_THRESHOLD

        if is_anomaly:
            if sensor == "temperature":
                val = random.choice([10.0, 46.0])
            elif sensor == "humidity":
                val = random.choice([20.0, 96.0])
            else:
                val = random.choice([970.0, 1050.0])
        else:
            val = round(random.uniform(min_val, max_val), 2)

        msg = {
            "sensor": sensor,
            "value": val,
            "unit": {"temperature":"C","humidity":"%","pressure":"hPa"}[sensor],
            "timestamp": int(time.time() * 1000),
            "source": args.source,
            "anomaly": is_anomaly
        }
        producer.send("sensor-events", key=sensor, value=msg)
        time.sleep(interval)

    producer.flush()
    producer.close()
    print(f"Sent {args.count} messages")

if __name__ == "__main__":
    main()