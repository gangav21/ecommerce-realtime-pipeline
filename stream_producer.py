import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def generate_clickstream():
    events = ['view_item', 'add_to_cart', 'purchase']
    return {
        "user_id": random.randint(1000, 9999),
        "event": random.choice(events),
        "product_id": random.randint(1, 500),
        "timestamp": int(time.time())
    }

while True:
    data = generate_clickstream()
    producer.send('clickstream', json.dumps(data).encode('utf-8'))
    print(f"Sent: {data}")
    time.sleep(1) # Simulates real-time traffic
