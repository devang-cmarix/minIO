import json
import tempfile
import pika
import time
from .config import settings
from .logger import get_logger
from .minio_client import client as minio_client
from .pdf_processor import extract_text_from_pdf_bytes
from .db import collection

logger = get_logger("pdf_processor")


def process_event(event):
    record = event["Records"][0]
    object_name = record["s3"]["object"]["key"]

    logger.info(f"Processing PDF: {object_name}")

    # idempotency check
    if collection.find_one({"file": object_name}):
        logger.info("Already processed, skipping.")
        return

    # download file as bytes
    response = minio_client.get_object(
        settings.MINIO_BUCKET,
        object_name
    )
    pdf_bytes = response.read()
    response.close()
    response.release_conn()

    text = extract_text_from_pdf_bytes(pdf_bytes)

    collection.insert_one({
        "file": object_name,
        "text": text
    })

    logger.info("Stored in MongoDB.")



def start_consumer():
    params = pika.URLParameters(settings.RABBITMQ_URL)

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=settings.QUEUE_NAME, durable=True)

            def callback(ch, method, properties, body):
                try:
                    event = json.loads(body)
                    process_event(event)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception:
                    logger.exception("Processing failed")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_consume(
                queue=settings.QUEUE_NAME,
                on_message_callback=callback
            )

            logger.info("PDF processor started.")
            channel.start_consuming()

        except Exception as e:
            logger.error(f"Connection lost: {e}")
            time.sleep(5)
