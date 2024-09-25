import env_var
import asyncio
import logging
from typing import Dict
from data_collection import DataCollector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def collecting_data(eodhd_api_token: str, mongo_uri: str):
    failed_operations: Dict[str, Dict[str, Exception]] = {}
    
    async with DataCollector(eodhd_api_token, mongo_uri) as dc:
        symbols = ['AAPL', 'TSLA', 'MSFT']
        indices = ['GSPC.INDX']
        country = ['USA']

        tasks = {
            'historical': [dc.collect_and_store_historical_data(symbol) for symbol in symbols],
            'fundamental': [dc.collect_and_store_fundamental_data(symbol) for symbol in symbols],
            'news': [dc.collect_and_store_news_data(symbol) for symbol in symbols],
            'earnings': [dc.collect_and_store_earnings_data()],
            'trends': [dc.collect_and_store_trends_data(symbols)],
            'ipos': [dc.collect_and_store_ipos_data()],
            'splits': [dc.collect_and_store_splits_data()],
            'macro_indicators': [dc.collect_and_store_macro_indicators_data(country) for country in country],
            'indices': [dc.collect_and_store_indices_data(index) for index in indices]
        }
        
        results = dict(
            zip(
                tasks.keys(),
                await asyncio.gather(
                    *(asyncio.gather(*task_list, return_exceptions=True) for task_list in tasks.values())
                )
            )
        )

    for task_type, task_results in results.items():
        if task_type == "indices":
            for index, result in zip(indices, task_results):
                if isinstance(result, Exception):
                    failed_operations.setdefault(task_type, {})[index] = result
        elif task_type in ['historical', 'fundamental', 'news']:
            for symbol, result in zip(symbols, task_results):
                if isinstance(result, Exception):
                    failed_operations.setdefault(task_type, {})[symbol] = result
        elif task_type in ['earnings', 'trends', 'ipos', 'splits', 'macro_indicators']:
            if isinstance(task_results[0], Exception):
                failed_operations.setdefault(task_type, {})['all_data'] = task_results[0]

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
    eodhd_api_token = env_var.EODHD_REAL_TOKEN
    mongo_uri = f"mongodb://{env_var.MONGO_HOST}:27017/"
    failed_operations = await collecting_data(eodhd_api_token, mongo_uri)

if __name__ == "__main__":
    asyncio.run(main())