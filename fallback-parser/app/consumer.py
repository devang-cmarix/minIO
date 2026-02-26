import pika
import json
import time
from .config import settings
from .fallback import fallback_parse
from .publisher import publish_to_review
from .logger import get_logger
from .publisher import publish_to_main

logger = get_logger("fallback")


def start():
    params = pika.URLParameters(settings.RABBITMQ_URL)

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            # channel.queue_declare(queue=settings.FALLBACK_QUEUE, durable=True)
            channel.queue_declare(
            queue=settings.FALLBACK_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": settings.REVIEW_QUEUE
            }
)

            from bson import ObjectId
            from .db import collection  # make sure this exists

            def callback(ch, method, properties, body):

                msg = json.loads(body)
                doc_id = msg.get("doc_id")

                data = msg.get("data")

                if not data and "structured_data" in msg:
                    data = msg.get("structured_data")

                if not data:
                    data = msg

                new_data = fallback_parse(data)

                if not doc_id:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                # ----------------------------------------
                # CASE 1: Successfully corrected
                # ----------------------------------------
                if (
                    new_data.get("invoice_number") != "UNKNOWN"
                    and new_data.get("total", 0) > 0
                ):

                    collection.update_one(
                        {"_id": ObjectId(doc_id)},
                        {
                            "$set": {
                                "structured_data": new_data,
                                "pipeline.validated": True,
                                "pipeline.fallback": False,
                                "status": "validated"
                            }
                        }
                    )

                    publish_to_main({
                        "doc_id": doc_id
                    })

                    logger.info("Fallback recovered and sent to main pipeline")

                # ----------------------------------------
                # CASE 2: Still invalid → send to review
                # ----------------------------------------
                else:

                    collection.update_one(
                        {"_id": ObjectId(doc_id)},
                        {
                            "$set": {
                                "pipeline.manual_review": True,
                                "status": "manual_review"
                            }
                        }
                    )

                    publish_to_review({
                        "doc_id": doc_id
                    })

                    logger.info("Fallback failed → sent to manual review")

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
