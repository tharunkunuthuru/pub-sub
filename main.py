from google.cloud import pubsub_v1
from flask import Flask
import threading
import time
import os

PROJECT_ID = os.environ.get("PROJECT_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID")

app = Flask(__name__)

def pull_messages():
    print("✅ Pull thread started")
    print(f"🔧 Using PROJECT_ID = {PROJECT_ID}, SUBSCRIPTION_ID = {SUBSCRIPTION_ID}")

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    print(f"🧪 Subscribing to: {subscription_path}")

    while True:
        try:
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 5},
                timeout=10
            )
            if not response.received_messages:
                print("📭 No messages received. Retrying...")
            for msg in response.received_messages:
                print(f"📩 Received: {msg.message.data.decode('utf-8')}")
                subscriber.acknowledge(
                    request={"subscription": subscription_path, "ack_ids": [msg.ack_id]}
                )
        except Exception as e:
            print(f"⚠️ Error during pull: {e}")
        time.sleep(5)

@app.route("/")
def home():
    return "Pull worker running!", 200

if __name__ == "__main__":
    print("🚀 Starting Flask app and pull thread...")
    threading.Thread(target=pull_messages, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
