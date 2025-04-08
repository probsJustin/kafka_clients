#!/usr/bin/env python3

import os
import json
import time
import logging
import signal
import sys
from typing import Dict, Any, Optional, List
from confluent_kafka import Consumer, Producer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default configuration that can be overridden with environment variables
DEFAULT_CONFIG = {
    "KAFKA_BROKER": "3.128.168.139:9092",
    "KAFKA_TOPIC": "test",
    "KAFKA_CONSUMER_GROUP": "kubernetes-consumer-group",
    "KAFKA_MESSAGE": "Hello from Kubernetes pod",
    "PRODUCER_INTERVAL_SECONDS": "10",
    "MAX_POLL_INTERVAL_MS": "300000",  # 5 minutes
    "AUTO_OFFSET_RESET": "earliest"
}


def get_config() -> Dict[str, str]:
    """Load configuration from environment variables with defaults."""
    config = DEFAULT_CONFIG.copy()
    
    # Override defaults with environment variables if they exist
    for key in config:
        if key in os.environ:
            config[key] = os.environ[key]
    
    logger.info(f"Using configuration: {json.dumps({k: v for k, v in config.items() if 'PASSWORD' not in k})}")
    return config


def delivery_report(err: Optional[Exception], msg: Any) -> None:
    """Callback invoked on message delivery success or failure."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def create_producer(config: Dict[str, str]) -> Producer:
    """Create a Kafka producer from config."""
    producer_config = {
        'bootstrap.servers': config['KAFKA_BROKER'],
        'security.protocol': 'PLAINTEXT',
        'receive.message.max.bytes': 1000000000,  # 1GB
        'message.max.bytes': 1000000000,  # 1GB
        'api.version.request': 'true',
    }
    
    logger.info(f"Creating producer connecting to Kafka at: {config['KAFKA_BROKER']}")
    return Producer(producer_config)


def create_consumer(config: Dict[str, str]) -> Consumer:
    """Create a Kafka consumer from config."""
    consumer_config = {
        'bootstrap.servers': config['KAFKA_BROKER'],
        'group.id': config['KAFKA_CONSUMER_GROUP'],
        'auto.offset.reset': config['AUTO_OFFSET_RESET'],
        'max.poll.interval.ms': config['MAX_POLL_INTERVAL_MS'],
        'fetch.max.bytes': 1000000000,
        'receive.message.max.bytes': 1000001000,
        'message.max.bytes': 1000000000,
        'security.protocol': 'PLAINTEXT',
        'api.version.request': 'true',
    }
    
    logger.info(f"Creating consumer connecting to Kafka at: {config['KAFKA_BROKER']}")
    return Consumer(consumer_config)


def publish_message(producer: Producer, config: Dict[str, str]) -> None:
    """Publish a message to Kafka topic."""
    topic = config['KAFKA_TOPIC']
    message = config['KAFKA_MESSAGE']
    
    try:
        producer.produce(
            topic,
            value=message.encode('utf-8'),
            callback=delivery_report
        )
        # Wait for any outstanding messages to be delivered
        producer.flush()
    except Exception as e:
        logger.error(f"Failed to publish message: {e}")


def consume_messages(consumer: Consumer, topics: List[str]) -> None:
    """Consume a message from Kafka topic."""
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            return
        
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            return
            
        value = msg.value().decode('utf-8')
        logger.info(f"Received message: {value}")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")


def setup_signal_handling(producer: Producer, consumer: Consumer) -> None:
    """Setup signal handlers for graceful shutdown."""
    def signal_handler(sig, frame):
        logger.info("Shutting down...")
        consumer.close()
        producer.flush()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main() -> None:
    """Main entry point for the Kafka client."""
    config = get_config()
    
    # Create producer and consumer
    producer = create_producer(config)
    consumer = create_consumer(config)
    
    # Subscribe to topic
    topic = config['KAFKA_TOPIC']
    consumer.subscribe([topic])
    logger.info(f"Subscribed to topic: {topic}")
    
    # Setup signal handling for graceful shutdown
    setup_signal_handling(producer, consumer)
    
    # Main loop - produce and consume messages
    interval = int(config['PRODUCER_INTERVAL_SECONDS'])
    last_produce_time = 0
    
    logger.info(f"Starting main loop. Producing every {interval} seconds.")
    try:
        while True:
            # Produce message at specified interval
            current_time = time.time()
            if current_time - last_produce_time >= interval:
                publish_message(producer, config)
                last_produce_time = current_time
            
            # Consume any available messages
            consume_messages(consumer, [topic])
            
            # Small delay to prevent CPU spinning
            time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        consumer.close()
        producer.flush()
        logger.info("Kafka client shut down")


if __name__ == "__main__":
    main()