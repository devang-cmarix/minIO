import json
import pika
import time
from .config import settings
from .logger import get_logger

logger = get_logger("publisher")

class EventPublisher:
    def __init__(self):
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        
        params = pika.URLParameters(settings.RABBITMQ_URL)
        params.heartbeat = 300  # 5 minutes
        params.blocked_connection_timeout = 300
        
        while True:
            try:
                logger.info("Connecting to RabbitMQ...")
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                # self.channel.queue_declare(
                #     queue=settings.QUEUE_NAME,
                #     durable=True
                # )
                self.channel.queue_declare(
                    queue=settings.QUEUE_NAME,
                    durable=True,
                    arguments={
                        "x-dead-letter-exchange": "",
                        "x-dead-letter-routing-key": "file.upload.dlq"
                    }
                )

                self.channel.queue_declare(
                    queue="file.upload.dlq",
                    durable=True
                )

                logger.info("Connected to RabbitMQ")
                break
            except Exception as e:
                logger.error(f"RabbitMQ not ready: {e}")
                time.sleep(5)

    def _ensure_connection(self):
        """Reconnect if channel or connection is closed."""
        if (
            self.connection is None
            or self.connection.is_closed
            or self.channel is None
            or self.channel.is_closed
        ):
            logger.warning("RabbitMQ connection lost. Reconnecting...")
            self._connect()
            
    def publish(self, message: dict):
        """Publish message with auto-reconnect."""
        for attempt in range(3):
            try:
                self._ensure_connection()

                self.channel.basic_publish(
                    exchange="",
                    routing_key=settings.QUEUE_NAME,
                    body=json.dumps(message),
                    properties=pika.BasicProperties(
                        delivery_mode=2
                    )
                )

                logger.info(f"Published event: {message}")
                return

            except Exception as e:
                logger.error(f"Publish failed (attempt {attempt+1}): {e}")
                time.sleep(2)
                self._connect()

        logger.error("Failed to publish message after retries.")
