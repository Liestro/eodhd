from async_eodhd_api import EodhdAPISession
import db_operations as db
import env_var
import asyncio
from pymongo import MongoClient

def connect_to_database() -> MongoClient:
    mongo_client = db.get_mongo_client()
    db.test_mongo_connection(mongo_client)
    return mongo_client

async def collecting_data(eodhd_api_token: str, mongo_client: MongoClient):
    async with EodhdAPISession(eodhd_api_token) as session:
        symbols = await session.get_exchange_symbols("NYSE")
        # symbols = ['TSLA', 'AAPL', 'MSFT']  # Tickers for demo api_token for testing
        historical_data_tasks = [session.get_historical_data(symbol) for symbol in symbols]
        news_data_tasks = [session.get_news_data(symbol) for symbol in symbols]
        fundamental_data_tasks = [session.get_fundamental_data(symbol) for symbol in symbols]
        
        historical_data, news_data, fundamental_data = await asyncio.gather(
            asyncio.gather(*historical_data_tasks),
            asyncio.gather(*news_data_tasks),
            asyncio.gather(*fundamental_data_tasks)
        )
        
        for symbol, data in historical_data:
            db.store_historical_data(mongo_client, symbol, data)
        
        for symbol, data in news_data:
            db.store_news_data(mongo_client, symbol, data)
        
        for symbol, data in fundamental_data:
            db.store_fundamental_data(mongo_client, symbol, data)

async def main():
    # Подключение к базе
    mongo_client = connect_to_database()
    
    eodhd_api_token = env_var.EODHD_API_TOKEN
    
    await collecting_data(eodhd_api_token, mongo_client)

if __name__ == "__main__":
    asyncio.run(main())