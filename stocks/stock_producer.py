import json
from confluent_kafka import Producer
from config.config import settings
from logger import logger


producer_config = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": settings.KAFKA_USERNAME,
    "sasl.password": settings.KAFKA_PASSWORD,
    "session.timeout.ms": 45000,
}
producer = Producer(**producer_config)


async def publish_stock_data(symbol, data):
    """Publish stock data to Kafka topic"""
    producer.send(settings.KAFKA_TOPIC, key=symbol, value=json.dumps(data))
    producer.flush()
    message = "Message published successfully to Kafka"
    logger.info(message)
    return {"status": 200, "message": message}


def shutdown_producer():
    """Flush and close the producer when shutting down."""
    logger.info("Shutting down producer...")
    producer.flush()
    producer.close()
    logger.info("Producer shut down successfully.")
