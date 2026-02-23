import pika
import json
import time
from .config import settings
from .fallback import fallback_parse
from .publisher import publish_to_review
from .logger import get_logger

logger = get_logger("fallback")


def start():
    params = pika.URLParameters(settings.RABBITMQ_URL)

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=settings.FALLBACK_QUEUE, durable=True)

            def callback(ch, method, properties, body):
                msg = json.loads(body)
                new_data = fallback_parse(msg["data"])

                if "invoice_number" in new_data:
                    logger.info("Recovered by fallback")
                else:
                    publish_to_review(msg)
                    logger.info("Sent to manual review")

                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(
                queue=settings.FALLBACK_QUEUE,
                on_message_callback=callback
            )

            logger.info("Fallback parser started")
            channel.start_consuming()

        except Exception as e:
            logger.error(f"RabbitMQ not ready: {e}")
            time.sleep(5)
