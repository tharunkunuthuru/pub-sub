import os
import time
import logging
from flask import Flask
from google.cloud import pubsub_v1

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.environ["GCP_PROJECT"]
SUBSCRIPTION_ID = os.environ["SUBSCRIPTION_ID"]

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

@app.route('/')
def pull_messages():
    response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": 5,
        }
    )

    ack_ids = []
    if response.received_messages:
        for received_message in response.received_messages:
            logging.info(f"Received message: {received_message.message.data.decode('utf-8')}")
            ack_ids.append(received_message.ack_id)

        # Acknowledge messages
        subscriber.acknowledge(
            request={
                "subscription": subscription_path,
                "ack_ids": ack_ids,
            }
        )
        return f"Acknowledged {len(ack_ids)} messages.\n"
    else:
        return "No messages to pull.\n"
