from async_eodhd_api import EodhdAPISession
import db_operations as db
import env_var
import asyncio
from pymongo import MongoClient, UpdateOne

def connect_to_database() -> MongoClient:
    mongo_client = db.get_mongo_client()
    db.test_mongo_connection(mongo_client)
    return mongo_client

async def collecting_data(eodhd_api_token: str, mongo_client: MongoClient):
    async with EodhdAPISession(eodhd_api_token) as session:
        # symbols = await session.get_exchange_symbols("NYSE")
        symbols = ['TSLA', 'AAPL', 'MSFT']
        historical_data_tasks = [session.get_historical_data(symbol) for symbol in symbols]
        news_data_tasks = [session.get_news_data(symbol) for symbol in symbols]
        
        historical_data, news_data = await asyncio.gather(
            asyncio.gather(*historical_data_tasks),
            asyncio.gather(*news_data_tasks)
        )
        
        for symbol, data in historical_data:
            if data:
                mongo_client.historical_data[symbol].create_index([("date", db.pymongo.ASCENDING)], unique=True)
                operations = [
                    UpdateOne({"date": item["date"]}, {"$set": item}, upsert=True)
                    for item in data
                ]
                mongo_client.historical_data[symbol].bulk_write(operations)
        
        for symbol, data in news_data:
            if data:
                mongo_client.news_data[symbol].create_index([("date", db.pymongo.ASCENDING)], unique=True)
                operations = [
                    UpdateOne({"date": item["date"]}, {"$set": item}, upsert=True)
                    for item in data
                ]
                mongo_client.news_data[symbol].bulk_write(operations)

async def main():
    # Подключение к базе
    mongo_client = connect_to_database()
    
    eodhd_api_token = env_var.EODHD_API_TOKEN
    
    await collecting_data(eodhd_api_token, mongo_client)

if __name__ == "__main__":
    asyncio.run(main())