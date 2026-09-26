from confluent_kafka import Consumer
import json
import time

def consumer_frontend():
    data = {}

    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "transaction" + str(time.time()),
        "auto.offset.reset": "earliest"
    }

    consumer = Consumer(config)
    consumer.subscribe(["transaction-credential"])

    start =time.time()
    
    try:
        while time.time() - start <= 5 :
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting transaction-credential ...")
            elif msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                print(
                    f"Consumed event key = {msg.key()} "
                    f"value = {msg.value().decode('utf-8')}"
                )
                data = json.loads(msg.value().decode("utf-8"))

    finally:
        consumer.close()

    return data
