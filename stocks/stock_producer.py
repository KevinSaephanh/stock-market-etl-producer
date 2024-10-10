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


class StockProducer:
    def __init__(self):
        self.producer = Producer(**producer_config)

    def publish_stock_data(self, symbol, data):
        """Publish stock data to Kafka topic"""
        self.producer.send(settings.KAFKA_TOPIC, key=symbol, value=json.dumps(data))
        self.producer.flush()
        message = "Message published successfully to Kafka"
        logger.info(message)
        return {"status": 200, "message": message}

    def close(self):
        """Flush and close the producer when shutting down."""
        logger.info("Shutting down producer...")
        self.producer.flush()
        self.producer.close()
        logger.info("Producer shut down successfully.")
