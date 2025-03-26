# kafka_clients

A Python client for interacting with Apache Kafka brokers. This repository contains a configurable script that can both consume from and produce to Kafka topics.

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Edit the `config.json` file to configure your Kafka connection:

```json
{
  "kafka": {
    "host": "localhost",
    "port": 9092,
    "topic": "test-topic",
    "message": "Hello, Kafka!"
  }
}
```

## Usage

### Produce a message

```bash
python kafka_client.py -c config.json -m produce
```

### Consume messages

```bash
python kafka_client.py -c config.json -m consume -n 10
```

### Both produce and consume

```bash
python kafka_client.py -c config.json -m both
```

### Options

- `-c, --config`: Path to configuration file (required)
- `-m, --mode`: Operation mode (`produce`, `consume`, or `both`)
- `-g, --group`: Consumer group ID (default: `my-consumer-group`)
- `-n, --num-messages`: Number of messages to consume (default: 5)
