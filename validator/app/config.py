class Settings:
    # RabbitMQ
    RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"
    VALIDATE_QUEUE = "ai.validate"
    FALLBACK_QUEUE = "ai.fallback"

    # MySQL (inside Docker network)
    MYSQL_HOST = "mysql"
    MYSQL_PORT = 3306
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "Admin@123#@!"
    MYSQL_DB = "documents"


settings = Settings()
