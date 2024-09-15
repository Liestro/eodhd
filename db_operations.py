from pymongo import MongoClient
import pymongo
import env_var


def get_mongo_client():
    mongo_uri = f"mongodb://{env_var.MONGO_HOST}:27017/"
    return MongoClient(mongo_uri)


def test_mongo_connection(mongo_client):
    try:
        mongo_client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)


def store_fundamentals_data(mongo_client, fundamental_data):
    mongo_client.eodhd.fundamentals_data.create_index([("name", pymongo.ASCENDING)], unique=True)
    fundamental_data["name"] = fundamental_data["General"]["Name"]
    if not mongo_client.eodhd.fundamentals_data.count_documents({"name": fundamental_data["name"]}):
        mongo_client.eodhd.fundamentals_data.insert_one(fundamental_data)


def store_news_data(mongo_client, news_data):
    mongo_client.eodhd.news.insert_many(news_data)