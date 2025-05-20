# main.py
import os
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1
import base64

app = Flask(__name__)

# Hardcoded Pub/Sub configuration (replace with your actual values)
PROJECT_ID = 'others-459904'
SUBSCRIPTION_ID = 'my-first-test-1595-sub'

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

@app.route('/')
def index():
    return "Pub/Sub Message Puller Application. Send a POST request to /pull-messages to pull messages."

@app.route('/pull-messages', methods=['GET', 'POST'])
def pull_messages():
    max_messages = 5  # Number of messages to pull at once
    pulled_messages = []

    try:
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": max_messages,
                "return_immediately": True
            }
        )

        ack_ids = []
        for received_message in response.received_messages:
            message_data = received_message.message.data.decode("utf-8")
            pulled_messages.append({
                "message_id": received_message.message.message_id,
                "data": message_data,
                "attributes": dict(received_message.message.attributes)
            })
            ack_ids.append(received_message.ack_id)

        if ack_ids:
            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": ack_ids
                }
            )
            return jsonify({
                "status": f"Successfully pulled and acknowledged {len(pulled_messages)} messages.",
                "messages": pulled_messages
            }), 200
        else:
            return jsonify({"status": "No messages to pull from the subscription."}), 200

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
