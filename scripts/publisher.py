import json
import time
import random
import requests
from google.cloud import pubsub_v1

PROJECT_ID = "YOUR_PROJECT_ID"
TOPIC_ID = "ecom-orders-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

API_BASE_URL = "https://fakestoreapi.com/carts"

def fetch_and_publish():
    try:
        cart_id = random.randint(1, 7)
        response = requests.get(f"{API_BASE_URL}/{cart_id}").json()
        response["ingested_at"] = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
        )
        payload = json.dumps(response).encode("utf-8")
        future = publisher.publish(topic_path, payload)
        print(
            f"Published Order Cart ID #{cart_id} to Pub/Sub. "
            f"Message ID: {future.result()}"
        )
    except Exception as e:
        print(f"Error publishing order: {e}")

if __name__ == "__main__":
    print("Starting E-Commerce Order Streamer to Pub/Sub... Press Ctrl+C to stop.")
    while True:
        fetch_and_publish()
        time.sleep(5)
