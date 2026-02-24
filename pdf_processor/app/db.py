from pymongo import MongoClient
from .config import settings

client = MongoClient(settings.MONGO_URL)
db = client[settings.DB_NAME]
collection = db[settings.COLLECTION]
