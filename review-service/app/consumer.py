import pika
import json
import time
from .config import settings
from .db import collection
from .logger import get_logger

logger = get_logger("review")


def start():
    params = pika.URLParameters(settings.RABBITMQ_URL)

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=settings.REVIEW_QUEUE, durable=True)

            def callback(ch, method, properties, body):
                msg = json.loads(body)
                collection.insert_one(msg)
                logger.info("Stored for manual review")
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(
                queue=settings.REVIEW_QUEUE,
                on_message_callback=callback
            )

            logger.info("Review service started")
            channel.start_consuming()

        except Exception as e:
            logger.error(f"RabbitMQ not ready: {e}")
            time.sleep(5)
