import pika
import json
from .config import settings

params = pika.URLParameters(settings.RABBITMQ_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue=settings.FALLBACK_QUEUE, durable=True)

def publish_to_fallback(message: dict):
    try:
        params = pika.URLParameters(settings.RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.queue_declare(queue=settings.FALLBACK_QUEUE, durable=True)

        channel.basic_publish(
            exchange="",
            routing_key=settings.FALLBACK_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2
            ),
        )

        connection.close()
        
    except Exception as e:
        print(f"Fallback publish failed: {e}")