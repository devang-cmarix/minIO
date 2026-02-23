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
            channel.queue_declare(queue=settings.VALIDATE_QUEUE, durable=True)
            return channel
        except Exception:
            time.sleep(5)


def publish(data):
    channel = get_channel()
    channel.basic_publish(
        exchange="",
        routing_key=settings.VALIDATE_QUEUE,
        body=json.dumps(data),
        properties=pika.BasicProperties(delivery_mode=2),
    )
