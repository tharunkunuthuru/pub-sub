# main.py
import os
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1
import base64

app = Flask(__name__)

# Environment variables for Pub/Sub configuration
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
SUBSCRIPTION_ID = os.environ.get('PUBSUB_SUBSCRIPTION_ID')

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

@app.route('/')
def index():
    return "Pub/Sub Message Puller Application. Send a POST request to /pull-messages to pull messages."

@app.route('/pull-messages', methods=['GET'])
def pull_messages():
    if not PROJECT_ID or not SUBSCRIPTION_ID:
        return jsonify({"error": "PROJECT_ID or PUBSUB_SUBSCRIPTION_ID environment variables are not set."}), 500

    max_messages = 5  # Number of messages to pull at once
    pulled_messages = []

    try:
        # The subscriber client is already initialized globally.
        # Use pull() method for synchronous pull.
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": max_messages,
                "return_immediately": True  # Returns immediately even if no messages
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
            # Acknowledge the messages to remove them from the subscription
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
    # This is for local development. Cloud Run will use gunicorn or similar.
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

###
