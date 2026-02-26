import pika
import json
import time
from datetime import datetime
from bson import ObjectId

from .config import settings
from .db import collection
from .logger import get_logger
from .mapping_engine import apply_mapping

logger = get_logger("review")


def start():
    params = pika.URLParameters(settings.RABBITMQ_URL)

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=settings.REVIEW_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)

            def callback(ch, method, properties, body):
                try:
                    msg = json.loads(body)
                    doc_id = msg.get("doc_id")

                    if not doc_id:
                        logger.warning("No doc_id in message")
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    # Convert to ObjectId if needed
                    document = collection.find_one({"_id": ObjectId(doc_id)})

                    if not document:
                        logger.warning(f"Document {doc_id} not found")
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    extracted_text = document.get("extracted_text", {})

                    # Apply mapping
                    mapped_output = apply_mapping(extracted_text)

                    # Update Mongo
                    collection.update_one(
                        {"_id": document["_id"]},
                        {
                            "$set": {
                                "review_output": mapped_output,
                                "review_generated_at": datetime.utcnow()
                            }
                        }
                    )

                    logger.info(f"Review mapping completed for {doc_id}")

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as inner_error:
                    logger.exception("Error processing review message")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(
                queue=settings.REVIEW_QUEUE,
                on_message_callback=callback
            )

            logger.info("Review service started")
            channel.start_consuming()

        except Exception as e:
            logger.error(f"RabbitMQ not ready: {e}")
            time.sleep(5)