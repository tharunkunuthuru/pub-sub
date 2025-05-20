from flask import Flask, render_template_string
from google.cloud import pubsub_v1
import os

app = Flask(__name__)

# Set environment variable GOOGLE_APPLICATION_CREDENTIALS in Cloud Run
project_id = os.getenv("GCP_PROJECT_ID")
subscription_id = os.getenv("PUBSUB_SUBSCRIPTION_ID")

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

HTML_TEMPLATE = """
<!doctype html>
<title>Pub/Sub Messages</title>
<h1>Messages from Pub/Sub</h1>
<ul>
  {% for msg in messages %}
    <li>{{ msg }}</li>
  {% endfor %}
</ul>
"""

@app.route("/")
def index():
    messages = []
    response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": 5,
        },
        timeout=5
    )
    ack_ids = []
    for msg in response.received_messages:
        messages.append(msg.message.data.decode("utf-8"))
        ack_ids.append(msg.ack_id)

    if ack_ids:
        subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})

    return render_template_string(HTML_TEMPLATE, messages=messages)

if __name__ == "__main__":
    app.run(debug=True)
