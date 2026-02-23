import os

class Settings:
    # RabbitMQ service name from docker-compose
    RABBITMQ_URL = os.getenv(
        "RABBITMQ_URL",
        "amqp://guest:guest@rabbitmq:5672/"
    )

    PARSE_QUEUE = "ai.parse"
    VALIDATE_QUEUE = "ai.validate"

    # Mongo service name from docker-compose
    MONGO_URL = os.getenv(
        "MONGO_URL",
        "mongodb://mongodb:27017/"
    )

    # Must match frontend
    DB_NAME = os.getenv("MONGO_DB", "files_db")
    COLLECTION = os.getenv("MONGO_COLL", "pdf_texts")
    
settings = Settings()
