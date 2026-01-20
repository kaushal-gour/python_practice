import json, time, uuid, random
from datetime import datetime
from kafka import KafkaProducer   # ✅ replaced

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "t2_test"

# Simulated bus data
busline = '00001'

def generate_message(i):
    data = {
        "busline": busline,
        "key": f"{busline}_{uuid.uuid4()}",
        "timestamp": str(datetime.utcnow()),
        "latitude": 19.07 + (i % 10) * 0.001,
        "longitude": 72.87 + (i % 10) * 0.001,
        "status": "BROKEN" if i % 10 == 0 else "RUNNING"
    }
    return data   # ✅ return dict, not JSON string

i = 0
while True:
    message = generate_message(i)
    producer.send(TOPIC, message)   # ✅ replaced
    print(f"Sent: {message}")
    i += 1
    time.sleep(1)
