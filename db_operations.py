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


def store_news_data(mongo_client, news_data):
    mongo_client.eodhd.news.insert_many(news_data)