from pymongo import MongoClient, UpdateOne
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


def store_historical_data(mongo_client, symbol, data):
    if data:
        mongo_client.historical_data[symbol].create_index([("date", pymongo.ASCENDING)], unique=True)
        operations = [
            UpdateOne({"date": item["date"]}, {"$set": item}, upsert=True)
            for item in data
        ]
        mongo_client.historical_data[symbol].bulk_write(operations)


def store_news_data(mongo_client, symbol, data):
    if data:
        mongo_client.news_data[symbol].create_index([("date", pymongo.ASCENDING)], unique=True)
        operations = [
            UpdateOne({"date": item["date"]}, {"$set": item}, upsert=True)
            for item in data
        ]
        mongo_client.news_data[symbol].bulk_write(operations)


def store_fundamental_data(mongo_client, symbol, data):
    if data:
        mongo_client.fundamental_data[symbol].replace_one({}, data, upsert=True)