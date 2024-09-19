from async_eodhd_api import EodhdAPISession
from db_operations import EodhdMongoClient
import env_var
import asyncio

def connect_to_database() -> EodhdMongoClient:
    mongo_uri = f"mongodb://{env_var.MONGO_HOST}:27017/"
    mongo_client = EodhdMongoClient(mongo_uri)
    mongo_client.test_connection()
    return mongo_client

async def collect_and_store_historical_data(session: EodhdAPISession, mongo_client: EodhdMongoClient, symbol: str):
    historical_data = await session.get_historical_data(symbol)
    mongo_client.store_historical_data(symbol, historical_data[1])


async def collect_and_store_fundamental_data(session: EodhdAPISession, mongo_client: EodhdMongoClient, symbol: str):
    fundamental_data = await session.get_fundamental_data(symbol)
    mongo_client.store_fundamental_data(symbol, fundamental_data[1])


async def collect_and_store_news_data(session: EodhdAPISession, mongo_client: EodhdMongoClient, symbol: str):
    news_data = await session.get_news_data(symbol)
    mongo_client.store_news_data(symbol, news_data[1])
    
    
async def collecting_data(eodhd_api_token: str, mongo_client: EodhdMongoClient):
    async with EodhdAPISession(eodhd_api_token) as session:
        # symbols = await session.get_exchange_symbols("NYSE")
        symbols = ['TSLA', 'AAPL', 'MSFT']  # Tickers for demo api_token for testing
        
        tasks = []
        for symbol in symbols:
            tasks.extend([
                collect_and_store_historical_data(session, mongo_client, symbol),
                collect_and_store_fundamental_data(session, mongo_client, symbol),
                collect_and_store_news_data(session, mongo_client, symbol)
            ])
        
        await asyncio.gather(*tasks)
        
        for symbol in symbols:
            print(f"All data for symbol {symbol} successfully collected and saved.")

async def main():
    # Подключение к базе
    mongo_client = connect_to_database()
    
    eodhd_api_token = env_var.EODHD_API_TOKEN
    
    await collecting_data(eodhd_api_token, mongo_client)

if __name__ == "__main__":
    asyncio.run(main())