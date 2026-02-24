import pika
import json
from .config import settings
from .validator import validate_invoice
from .mysql_client import insert_invoice
from .publisher import publish_to_fallback
from .logger import get_logger

logger = get_logger("validator")


def callback(ch, method, properties, body):
    msg = json.loads(body)

    data = msg["data"]
    if isinstance(data, str):
        data = json.loads(data)
    raw_text = msg.get("raw_text", "")
    file = msg["file"]

    valid, errors = validate_invoice(data)

    insert_invoice(file, data, raw_text)
    
    if errors:
        logger.warning(f"Invoice stored with validation issues: {errors}")
        
    if not valid:
        logger.warning("Stored with validation errors")

    # else:
    #     # minor correction
    #     items = data.get("items", [])
    #     if not data.get("subtotal") and items:
    #         data["subtotal"] = sum(i.get("amount", 0) for i in items)

    #     valid_after_fix, _ = validate_invoice(data)

    #     if valid_after_fix:
    #         insert_invoice(file, data, raw_text)
    #         logger.info("Stored corrected invoice in MySQL")
    #     else:
    #         publish_to_fallback(msg)
    #         logger.info("Sent to fallback parser")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def start():
    connection = pika.BlockingConnection(
        pika.URLParameters(settings.RABBITMQ_URL)
    )
    channel = connection.channel()
    channel.queue_declare(queue=settings.VALIDATE_QUEUE, durable=True)

    channel.basic_consume(
        queue=settings.VALIDATE_QUEUE,
        on_message_callback=callback
    )

    logger.info("Validator started")
    channel.start_consuming()
