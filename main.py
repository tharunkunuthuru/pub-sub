# Python example (adding a health check to your existing pull worker)
from google.cloud import pubsub_v1
import os
import time
from flask import Flask

app = Flask(__name__)
port = int(os.environ.get('PORT', 8080))

subscription_id = os.environ.get("projects/others-459904/subscriptions/test-1595-2")
project_id = os.environ.get("others-459904")

def process_message(message_data):
    print(f"Processing message: {message_data.decode('utf-8')}")
    time.sleep(3)

def callback(message: pubsub_v1.subscriber.message.Message):
    print(f"Received message ID: {message.id}")
    process_message(message.data)
    message.ack()
    print(f"Acknowledged message ID: {message.id}")

def pubsub_pull_worker():
    if not subscription_id or not project_id:
        print("Error: PUBSUB_SUBSCRIPTION_ID and GOOGLE_CLOUD_PROJECT environment variables must be set.")
        return

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        print(f"Error: {e}")

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == '__main__':
    import threading
    threading.Thread(target=pubsub_pull_worker, daemon=True).start()
    app.run(host='0.0.0.0', port=port)
