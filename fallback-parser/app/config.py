class Settings:
    RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"
    FALLBACK_QUEUE = "ai.fallback"
    REVIEW_QUEUE = "ai.review"

settings = Settings()
