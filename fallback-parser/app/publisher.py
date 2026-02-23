import pika
import json
import time
from .config import settings


def get_channel():
    params = pika.URLParameters(settings.RABBITMQ_URL)

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=settings.REVIEW_QUEUE, durable=True)
            return channel
        except Exception:
            time.sleep(5)


def publish_to_review(msg):
    channel = get_channel()
    channel.basic_publish(
        exchange="",
        routing_key=settings.REVIEW_QUEUE,
        body=json.dumps(msg),
        properties=pika.BasicProperties(delivery_mode=2),
    )
