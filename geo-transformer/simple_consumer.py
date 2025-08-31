import os
import logging
from confluent_kafka import Consumer, KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('simple-consumer')

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPICS_PATTERN = "^dbserver1\\..*"  # All Debezium topics from MS SQL

# Configure Kafka consumer
consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'simple-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(consumer_config)

try:
    # Subscribe to topics matching pattern
    consumer.subscribe([TOPICS_PATTERN])
    logger.info(f"Subscribed to topics matching pattern: {TOPICS_PATTERN}")

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.debug(f"Reached end of partition {msg.partition()}")
            else:
                logger.error(f"Consumer error: {msg.error()}")
        else:
            # Log the message content
            logger.info(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    logger.info("Interrupted, closing consumer")
finally:
    consumer.close()
