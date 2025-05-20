FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENV GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT}
ENV PUBSUB_SUBSCRIPTION_ID=${PUBSUB_SUBSCRIPTION_ID}

# Expose the port that Cloud Run will send health checks to
EXPOSE 8080

# Command to run both the Pub/Sub pull worker and the Flask health check server
CMD ["sh", "-c", "python main.py & python -m flask run --host=0.0.0.0 --port=$PORT"]
