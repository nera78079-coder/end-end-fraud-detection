# views.py
from django.shortcuts import render
from django.http import HttpRequest, HttpResponse
from asgiref.sync import sync_to_async
from .kafka_producer_frontend import producer_frontend
import requests

async def index(request: HttpRequest) -> HttpResponse:
    data={"result":""}

    if request.method == "POST":
        await sync_to_async(producer_frontend)(request)
        data["status"] = "Message sent to Kafka"
        response = requests.get(
                    "http://127.0.0.1:8000/check/transaction",
                    params={"service": "django"}
                )
        data = response.json()
    return render(request, "home.html", {"data": data})
