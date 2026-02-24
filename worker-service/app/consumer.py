import json
import pika
from .config import settings
from .minio_client import get_minio_client
from .drive_downloader import DriveDownloader
from .logger import get_logger

logger = get_logger("worker")

class Worker:
    def __init__(self):
        self.minio = get_minio_client()
        self.downloader = DriveDownloader("credentials.json")

    def process(self, body):
        msg = json.loads(body)
        file_id = msg["file_id"]
        file_name = msg["file_name"]

        logger.info(f"Processing file: {file_name}")

        stream = self.downloader.download(file_id)

        self.minio.put_object(
            settings.MINIO_BUCKET,
            file_name,
            stream,
            length=-1,
            part_size=10 * 1024 * 1024
        )

        logger.info(f"Uploaded to MinIO: {file_name}")

import time

def start_worker():
    worker = Worker()
    params = pika.URLParameters(settings.RABBITMQ_URL)
    params.heartbeat = 300  # 5 minutes
    params.blocked_connection_timeout = 300

    while True:
        try:
            logger.info("Connecting to RabbitMQ...")
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            
            channel.queue_declare(
            queue=settings.QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": "file.upload.dlq"
            }
        )

            channel.queue_declare(
                queue="file.upload.dlq",
                durable=True
            )
            # def callback(ch, method, properties, body):
            #     try:
            #         worker.process(body)
            #         ch.basic_ack(delivery_tag=method.delivery_tag)
            #     except Exception:
            #         logger.exception("Worker failed")
            #         ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            def callback(ch, method, properties, body):
                msg = json.loads(body)

                retry_count = msg.get("retry", 0)

                try:
                    worker.process(body)   # your MinIO upload logic
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    logger.error(f"Upload failed: {e}")

                    if retry_count < 3:
                        msg["retry"] = retry_count + 1
                        channel.basic_publish(
                            exchange="",
                            routing_key="file.upload",
                            body=json.dumps(msg),
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        logger.warning(f"Retrying upload ({retry_count+1})")
                    else:
                        channel.basic_publish(
                            exchange="",
                            routing_key="file.upload.dlq",
                            body=json.dumps(msg),
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        logger.error("Moved to DLQ")

                    ch.basic_ack(delivery_tag=method.delivery_tag)



            channel.basic_consume(
                queue=settings.QUEUE_NAME,
                on_message_callback=callback
            )

            logger.info("Worker started. Waiting for messages...")
            channel.start_consuming()

        except Exception as e:
            logger.error(f"Connection lost: {e}")
            time.sleep(5)