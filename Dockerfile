FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kubernetes_kafka_client.py .

# Default config using environment variables
ENV KAFKA_BROKER="3.128.168.139:9092"
ENV KAFKA_TOPIC="test"
ENV KAFKA_CONSUMER_GROUP="kubernetes-consumer-group"
ENV KAFKA_MESSAGE="Hello from Kubernetes pod"
ENV PRODUCER_INTERVAL_SECONDS="10"

CMD ["python", "kubernetes_kafka_client.py"]