import time
import pika
from .consumer import start
from .config import settings
from .logger import get_logger

logger = get_logger("ai-parser")


def wait_for_rabbitmq():
    while True:
        try:
            params = pika.URLParameters(settings.RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            connection.close()
            logger.info("Connected to RabbitMQ")
            return
        except Exception as e:
            logger.error(f"RabbitMQ not ready: {e}")
            time.sleep(5)


def run():
    wait_for_rabbitmq()
    logger.info("AI parser started")
    start()   # ← CRITICAL: start consumer loop


if __name__ == "__main__":
    run()
