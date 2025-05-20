import os
import time
import json
from flask import Flask, jsonify
from google.cloud import pubsub_v1
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Get environment variables
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
SUBSCRIPTION_ID = os.environ.get('PUBSUB_SUBSCRIPTION_ID')

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def process_message(message):
    """Process the Pub/Sub message"""
    try:
        data = message.data.decode('utf-8')
        logger.info(f"Received message: {data}")
        
        # Process the message (add your business logic here)
        # For example, you could parse JSON data:
        try:
            json_data = json.loads(data)
            logger.info(f"Processed JSON data: {json_data}")
        except json.JSONDecodeError:
            logger.warning("Message is not valid JSON")
        
        # Acknowledge the message
        message.ack()
        return True
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Nack the message so it can be reprocessed
        message.nack()
        return False

def pull_messages(max_messages=5):
    """Pull messages from Pub/Sub subscription"""
    try:
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": max_messages,
            }
        )
        
        if not response.received_messages:
            return {"messages_processed": 0}
        
        processed_count = 0
        for received_message in response.received_messages:
            if process_message(received_message.message):
                processed_count += 1
                
        return {"messages_processed": processed_count}
    except Exception as e:
        logger.error(f"Error pulling messages: {e}")
        return {"error": str(e)}

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"})

@app.route('/pull', methods=['GET'])
def pull():
    """Endpoint to trigger message pulling"""
    result = pull_messages()
    return jsonify(result)

@app.route('/')
def home():
    """Home endpoint"""
    return jsonify({
        "service": "Pub/Sub Pull Service",
        "status": "running",
        "endpoints": {
            "/pull": "Pull messages from subscription",
            "/health": "Health check"
        }
    })

def run_subscriber_loop():
    """Background task to continuously pull messages"""
    while True:
        try:
            pull_messages(10)
            time.sleep(10)  # Wait 10 seconds between pulls
        except Exception as e:
            logger.error(f"Error in subscriber loop: {e}")
            time.sleep(30)  # Wait longer if there was an error

if __name__ == '__main__':
    # Get port from environment variable or default to 8080
    port = int(os.environ.get('PORT', 8080))
    
    # Start the subscriber loop in a separate thread
    import threading
    subscriber_thread = threading.Thread(target=run_subscriber_loop, daemon=True)
    subscriber_thread.start()
    
    # Start the Flask server
    app.run(host='0.0.0.0', port=port, debug=False)
