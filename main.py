import os
import time
import json
import sys
from flask import Flask, jsonify
from google.cloud import pubsub_v1
import logging

# Configure logging to output to stdout for Cloud Run logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Store last 100 messages for debug purposes
latest_messages = []
MAX_STORED_MESSAGES = 100

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
        attributes = message.attributes
        message_id = message.message_id
        
        logger.info(f"Received message with ID: {message_id}")
        logger.info(f"Message data: {data}")
        
        if attributes:
            logger.info(f"Message attributes: {attributes}")
        
        # Store message for debugging
        message_record = {
            "id": message_id,
            "data": data,
            "attributes": attributes,
            "received_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        latest_messages.append(message_record)
        if len(latest_messages) > MAX_STORED_MESSAGES:
            latest_messages.pop(0)
        
        # Process the message (add your business logic here)
        # For example, you could parse JSON data:
        try:
            json_data = json.loads(data)
            logger.info(f"Processed JSON data: {json_data}")
        except json.JSONDecodeError:
            logger.warning("Message is not valid JSON, treating as plain text")
        
        # Acknowledge the message
        message.ack()
        logger.info(f"Successfully acknowledged message: {message_id}")
        return True
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        # Nack the message so it can be reprocessed
        message.nack()
        return False

def pull_messages(max_messages=5):
    """Pull messages from Pub/Sub subscription"""
    try:
        logger.info(f"Pulling up to {max_messages} messages from subscription: {SUBSCRIPTION_ID}")
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": max_messages,
            }
        )
        
        if not response.received_messages:
            logger.info("No messages available to pull at this time")
            return {"messages_processed": 0}
        
        message_count = len(response.received_messages)
        logger.info(f"Pulled {message_count} messages from Pub/Sub")
        
        processed_count = 0
        for received_message in response.received_messages:
            if process_message(received_message.message):
                processed_count += 1
                
        logger.info(f"Successfully processed {processed_count} out of {message_count} messages")
        return {"messages_processed": processed_count, "messages_received": message_count}
    except Exception as e:
        logger.error(f"Error pulling messages: {e}", exc_info=True)
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

@app.route('/messages')
def view_messages():
    """Endpoint to view the latest processed messages"""
    return jsonify({
        "count": len(latest_messages),
        "max_stored": MAX_STORED_MESSAGES,
        "messages": latest_messages
    })

@app.route('/')
def home():
    """Home endpoint"""
    return jsonify({
        "service": "Pub/Sub Pull Service",
        "status": "running",
        "endpoints": {
            "/pull": "Pull messages from subscription",
            "/health": "Health check",
            "/messages": "View latest processed messages"
        }
    })

def run_subscriber_loop():
    """Background task to continuously pull messages"""
    logger.info(f"Starting background subscriber loop for subscription: {SUBSCRIPTION_ID}")
    while True:
        try:
            pull_messages(10)
            time.sleep(10)  # Wait 10 seconds between pulls
        except Exception as e:
            logger.error(f"Error in subscriber loop: {e}", exc_info=True)
            logger.info("Subscriber loop will retry in 30 seconds")
            time.sleep(30)  # Wait longer if there was an error

if __name__ == '__main__':
    # Get port from environment variable or default to 8080
    port = int(os.environ.get('PORT', 8080))
    
    # Log the startup information
    logger.info("="*80)
    logger.info("STARTING PUB/SUB PULL SERVICE")
    logger.info(f"Project ID: {PROJECT_ID}")
    logger.info(f"Subscription ID: {SUBSCRIPTION_ID}")
    logger.info(f"Subscription path: {subscription_path}")
    logger.info("="*80)
    
    # Start the subscriber loop in a separate thread
    import threading
    subscriber_thread = threading.Thread(target=run_subscriber_loop, daemon=True)
    subscriber_thread.start()
    logger.info("Background subscriber thread started")
    
    # Start the Flask server
    logger.info(f"Starting Flask server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
