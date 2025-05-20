from google.cloud import pubsub_v1
from flask import Flask
import threading
import time
import os

PROJECT_ID = os.environ.get("others-459904")
SUBSCRIPTION_ID = os.environ.get("projects/others-459904/subscriptions/first-test1595-sub")

app = Flask(__name__)

def pull_messages():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    while True:
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 5,
            },
            timeout=10
        )
        for msg in response.received_messages:
            print(f"Received: {msg.message.data.decode('utf-8')}")
            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": [msg.ack_id],
                }
            )
        time.sleep(10)

# Dummy endpoint for health check
@app.route("/")
def index():
    return "Pull worker running!", 200

if __name__ == "__main__":
    # Start pull_messages in a background thread
    threading.Thread(target=pull_messages, daemon=True).start()
    # Start HTTP server to make Cloud Run happy
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
