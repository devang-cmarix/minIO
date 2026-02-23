import pika
import json
from bson.objectid import ObjectId
from .config import settings
from .db import collection
from .parser import ai_parse
from .publisher import publish
from .logger import get_logger

logger = get_logger("ai-parser")

from bson.objectid import ObjectId

def callback(ch, method, properties, body):
    msg = json.loads(body)
    doc_id = msg["doc_id"]

    logger.info(f"Received doc_id: {doc_id}")

    try:
        record = collection.find_one({"_id": ObjectId(doc_id)})
    except Exception:
        logger.error("Invalid ObjectId format")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    if not record:
        logger.error("Document not found in MongoDB")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    text = record.get("extracted_text", "")
    parsed = ai_parse(text)

    # Update MongoDB
    collection.update_one(
        {"_id": ObjectId(doc_id)},
        {
            "$set": {
                "structured_data": parsed,
                "pipeline.ai_parsed": True
            }
        }
    )

    publish({
        "doc_id": doc_id,
        "file": record["filename"],
        "data": parsed,
        "raw_text": text
    })

    logger.info("AI parsing completed")

    ch.basic_ack(delivery_tag=method.delivery_tag)



def start():
    params = pika.URLParameters(settings.RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue=settings.PARSE_QUEUE, durable=True)

    logger.info("Waiting for messages...")

    channel.basic_consume(
        queue=settings.PARSE_QUEUE,
        on_message_callback=callback,
        auto_ack=False
    )

    channel.start_consuming()
