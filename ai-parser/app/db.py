from pymongo import MongoClient
from .config import settings

print("DEBUG MONGO_URL:", settings.MONGO_URL)
print("DEBUG DB:", settings.DB_NAME)
print("DEBUG COLLECTION:", settings.COLLECTION)

client = MongoClient(settings.MONGO_URL)
db = client[settings.DB_NAME]
collection = db[settings.COLLECTION]
