from confluent_kafka import Producer
import json

def delivery_callback(err, msg):
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        print(f"Produced message to {msg.topic()}")

def producer_frontend (request):

    
    config = {
        "bootstrap.servers": "localhost:9092",
    }

    producer = Producer(config)

    data = {
        "step": request.POST.get("step"), 
        "type": request.POST.get("type"),
        "amount": request.POST.get("amount"),
        "nameOrig": request.POST.get("nameOrig"),
        "oldbalanceOrg": request.POST.get("oldbalanceOrg"),
        "newbalanceOrig": request.POST.get("newbalanceOrig"),
        "nameDest": request.POST.get("nameDest"),
        "oldbalanceDest": request.POST.get("oldbalanceDest"),
        "newbalanceDest": request.POST.get("newbalanceDest"),
        "isFlaggedFraud": request.POST.get("isFlaggedFraud"),

    }

    producer.produce(
        "transaction-credential",
        value=json.dumps(data).encode("utf-8"),
        callback=delivery_callback
    )

    producer.flush()