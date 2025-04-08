#!/usr/bin/env python3

import json
import argparse
import logging
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, Producer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_file: str) -> Dict[str, Any]:
    """Load configuration from a JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        raise


def delivery_report(err: Optional[Exception], msg: Any) -> None:
    """Callback invoked on message delivery success or failure."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def create_producer(config: Dict[str, Any]) -> Producer:
    """Create a Kafka producer from config."""
    host = config['kafka']['host']
    port = config['kafka'].get('port', '9092')  # Default to 9092 if not specified
    path = config['kafka'].get('path', '')  # Path component like /kafka
    
    # Format bootstrap.servers based on whether we have a path
    server = host
    if port:
        server = f"{host}:{port}"
    if path:
        if path.startswith('/'):
            server = f"{server}{path}"
        else:
            server = f"{server}/{path}"
    
    producer_config = {
        'bootstrap.servers': server,
        'receive.message.max.bytes': 1000000000,  # 1GB (max allowed)
        'message.max.bytes': 1000000000,  # 1GB (max allowed)
        'security.protocol': 'SSL',
        'ssl.endpoint.identification.algorithm': 'none',
        'api.version.request': 'false',  # Skip API version request
        'broker.version.fallback': '2.0.0'  # Use a fallback version
    }
    
    logger.info(f"Connecting to Kafka server at: {server}")
    return Producer(producer_config)


def create_consumer(config: Dict[str, Any], group_id: str) -> Consumer:
    """Create a Kafka consumer from config."""
    host = config['kafka']['host']
    port = config['kafka'].get('port', '9092')  # Default to 9092 if not specified
    path = config['kafka'].get('path', '')  # Path component like /kafka
    
    # Format bootstrap.servers based on whether we have a path
    server = host
    if port:
        server = f"{host}:{port}"
    if path:
        if path.startswith('/'):
            server = f"{server}{path}"
        else:
            server = f"{server}/{path}"
    
    consumer_config = {
        'bootstrap.servers': server,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'fetch.max.bytes': 1000000000,
        'receive.message.max.bytes': 1000001000,  # 1GB (max allowed)
        'message.max.bytes': 1000000000,  # 1GB (max allowed)
        'security.protocol': 'SSL',
        'ssl.endpoint.identification.algorithm': 'none',
        'api.version.request': 'false',  # Skip API version request
        'broker.version.fallback': '2.0.0'  # Use a fallback version
    }
    
    logger.info(f"Connecting to Kafka server at: {server}")
    return Consumer(consumer_config)


def publish_message(producer: Producer, config: Dict[str, Any]) -> None:
    """Publish a message to Kafka topic."""
    topic = config['kafka']['topic']
    message = config['kafka'].get('message', 'Default message')
    
    try:
        producer.produce(
            topic,
            value=message.encode('utf-8'),
            callback=delivery_report
        )
        # Wait for any outstanding messages to be delivered
        producer.flush()
        logger.info(f"Published message to {topic}: {message}")
    except Exception as e:
        logger.error(f"Failed to publish message: {e}")


def consume_messages(consumer: Consumer, config: Dict[str, Any], max_messages: int = 5) -> None:
    """Consume messages from Kafka topic."""
    topic = config['kafka']['topic']
    consumer.subscribe([topic])
    logger.info(f"Subscribed to topic: {topic}")
    
    msg_count = 0
    try:
        while msg_count < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
                
            value = msg.value().decode('utf-8')
            logger.info(f"Received message: {value}")
            msg_count += 1
    except KeyboardInterrupt:
        logger.info("Consumption interrupted by user")
    finally:
        consumer.close()
        logger.info("Consumer closed")


def main() -> None:
    parser = argparse.ArgumentParser(description='Kafka producer and consumer client')
    parser.add_argument('-c', '--config', required=True, help='Path to config file')
    parser.add_argument('-m', '--mode', choices=['produce', 'consume', 'both'], 
                        default='both', help='Operation mode')
    parser.add_argument('-g', '--group', default='my-consumer-group', 
                        help='Consumer group ID')
    parser.add_argument('-n', '--num-messages', type=int, default=5,
                       help='Number of messages to consume (default: 5)')
    parser.add_argument('-p', '--port', 
                       help='Override Kafka port in config file')
    parser.add_argument('--path',
                       help='Override Kafka path component in config file (e.g., /kafka)')
    
    args = parser.parse_args()
    config = load_config(args.config)
    
    # Override port if provided via command line
    if args.port:
        config['kafka']['port'] = args.port
        
    # Override path if provided via command line
    if args.path:
        config['kafka']['path'] = args.path
    
    if args.mode in ['produce', 'both']:
        producer = create_producer(config)
        publish_message(producer, config)
    
    if args.mode in ['consume', 'both']:
        consumer = create_consumer(config, args.group)
        consume_messages(consumer, config, args.num_messages)


if __name__ == "__main__":
    main()