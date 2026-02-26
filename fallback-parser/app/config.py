class Settings:
    RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"
    FALLBACK_QUEUE = "ai.fallback"
    REVIEW_QUEUE = "ai.review"
    MONGO_URL = "mongodb://mongodb:27017"
    DB_NAME = "files_db"
    COLLECTION = "pdf_texts"
settings = Settings()
