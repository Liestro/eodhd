from async_eodhd_api import EodhdAPISession
from db_operations import EodhdMongoClient
import env_var
import asyncio
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_database() -> EodhdMongoClient:
    mongo_uri = f"mongodb://{env_var.MONGO_HOST}:27017/"
    # mongo_uri = f"mongodb://localhost:27017/"
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

# New function to collect and store index data
async def collect_and_store_indices_data(session: EodhdAPISession, mongo_client: EodhdMongoClient, index: str):
    index_data = await session.get_index_data(index)
    mongo_client.store_historical_data(index, index_data[1])

async def collecting_data(eodhd_api_token: str, mongo_client: EodhdMongoClient):
    failed_operations: Dict[str, Dict[str, Exception]] = {}
    
    async with EodhdAPISession(eodhd_api_token) as session:
        # symbols = session.get_exchange_symbols("NYSE")
        # symbols = ['TSLA', 'AAPL', 'MSFT', 'T', 'hvjhygtvjhgghjv'] # symbols accessible with demo api key / for testing
        symbols = ['TSLA']
        indices = ['GSPC.INDX']

        tasks = {
            'historical': [collect_and_store_historical_data(session, mongo_client, symbol) for symbol in symbols],
            # 'fundamental': [collect_and_store_fundamental_data(session, mongo_client, symbol) for symbol in symbols],
            'news': [collect_and_store_news_data(session, mongo_client, symbol) for symbol in symbols],
            'indices': [collect_and_store_indices_data(session, mongo_client, index) for index in indices]  # Add indices data collection
        }
        
        results = await asyncio.gather(
            *(asyncio.gather(*task_list, return_exceptions=True) for task_list in tasks.values())
        )
    
    for data_type, task_results in zip(tasks.keys(), results):
        if data_type == "indices":
            for index, result in zip(indices, task_results):
                if isinstance(result, Exception):
                    failed_operations.setdefault(data_type, {})[index] = result
        else:
            for symbol, result in zip(symbols, task_results):
                if isinstance(result, Exception):
                    failed_operations.setdefault(data_type, {})[symbol] = result
        

    # Log the results of the operation
    if failed_operations:
        logging.error("The following operations failed:")
        for data_type, failures in failed_operations.items():
            logging.error(f"Failed {data_type} operations:")
            for symbol_or_index, error in failures.items():
                logging.error(f"  - {symbol_or_index}: {str(error)}")
    else:
        logging.info("All operations completed successfully.")
    
    return failed_operations

async def main():
    mongo_client = connect_to_database()
    eodhd_api_token = env_var.EODHD_API_TOKEN
    failed_operations = await collecting_data(eodhd_api_token, mongo_client)

if __name__ == "__main__":
    asyncio.run(main())