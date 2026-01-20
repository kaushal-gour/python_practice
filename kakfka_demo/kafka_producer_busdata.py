import json, time, uuid, random
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP = "localhost:9092"
TOPIC = "t1_test"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

BUSLINES = ["00001", "00002", "00003"]
STATUS = ["RUNNING", "DELAYED", "BROKEN"]
LAT_RANGE = (18.90, 19.20)
LON_RANGE = (72.75, 72.95)

def generate():
    return {
        "busline": random.choice(BUSLINES),
        "key": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "latitude": round(random.uniform(*LAT_RANGE), 6),
        "longitude": round(random.uniform(*LON_RANGE), 6),
        "status": random.choice(STATUS)
    }

if __name__ == "__main__":
    print(f"Producing messages to Kafka topic '{TOPIC}'...")
    try:
        i = 1
        while True:
            data = generate()
            producer.send(TOPIC, data)
            print(f"Sent #{i}: {data}")
            time.sleep(1)
            i += 1
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()