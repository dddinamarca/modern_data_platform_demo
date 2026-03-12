import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

products = ["Laptop","Phone","Tablet","Monitor"]

while True:

    event = {
        "order_id": random.randint(10000,99999),
        "product": random.choice(products),
        "price": random.randint(100,2000),
        "quantity": random.randint(1,5),
        "event_time": int(time.time())
    }

    producer.send("sales_events", event)

    print(event)

    time.sleep(1)
