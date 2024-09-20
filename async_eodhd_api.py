import asyncio
from aiohttp import ClientSession, ClientError, ClientResponseError, ClientConnectorError
import time
import functools
import json
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def async_timer_decorator(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        logger.info(f"Starting function: {func.__name__}")
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Finished function: {func.__name__}")
        logger.info(f"Execution time: {end_time - start_time:.4f} seconds")
        return result
    return wrapper

class EodhdAPISession:
    def __init__(self, api_key: str, max_retries: int = 3, retry_delay: float = 1.0):
        self.api_key = api_key
        self.session = ClientSession(base_url='https://eodhd.com')
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.session.close()

    async def _make_request(self, endpoint: str, params: Dict[str, Any]):
        params['api_token'] = self.api_key
        params['fmt'] = 'json'
        url = f"{self.session._base_url}{endpoint}"

        for attempt in range(self.max_retries):
            try:
                async with self.session.get(endpoint, params=params) as resp:
                    resp.raise_for_status()
                    try:
                        return await resp.json()
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode JSON response: {str(e)}")
                        raise
            except ClientResponseError as e:
                logger.error(f"HTTP error occurred: {str(e)}")
                if e.status >= 500:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                    continue
                raise
            except ClientConnectorError as e:
                logger.error(f"Connection error occurred: {str(e)}")
                await asyncio.sleep(self.retry_delay * (2 ** attempt))
                continue
            except ClientError as e:
                logger.error(f"Client error occurred: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error occurred: {str(e)}")
                raise

        raise RuntimeError(f"Failed after {self.max_retries} attempts. URL: {url}, Params: {params}")

    @async_timer_decorator
    async def get_exchange_symbols(self, exchange: str):
        data = await self._make_request(f'/api/exchange-symbol-list/{exchange}', {})
        return [item['Code'] for item in data]

    @async_timer_decorator
    async def get_historical_data(self, symbol: str):
        data = await self._make_request(f'/api/eod/{symbol}', {'period': 'd'})
        logger.info(f"Received historical data for symbol {symbol}")
        return (symbol, data)

    @async_timer_decorator
    async def get_index_data(self, index: str):
        data = await self._make_request(f'/api/eod/{index}', {'period': 'd'})
        logger.info(f"Received historical data for index {index}")
        return (index, data)

    @async_timer_decorator
    async def get_fundamental_data(self, symbol: str):
        data = await self._make_request(f'/api/fundamentals/{symbol}', {})
        logger.info(f"Received fundamental data for symbol {symbol}")
        return (symbol, data)

    @async_timer_decorator
    async def get_news_data(self, symbol: str):
        data = await self._make_request('/api/news', {'s': symbol})
        logger.info(f"Received {len(data)} news articles for symbol {symbol}")
        return (symbol, data)

if __name__ == '__main__':
    async def main():
        start_time = time.time()
        api_key = "demo"
        async with EodhdAPISession(api_key) as api:
            try:
                results = await asyncio.gather(
                    # api.get_historical_data('TSLA'),
                    # api.get_fundamental_data('TSLA'),
                    # api.get_news_data('TSLA'),
                    # api.get_exchange_symbols('NYSE'),
                    api.get_index_data('GSPC.INDX'),  # S&P 500 index
                )
                
                # Output results
                logger.info(f"S&P 500 index data: {len(results[0][1])} records")
            except (ClientResponseError, ClientConnectorError, ClientError, json.JSONDecodeError, RuntimeError) as e:
                logger.error(f"Error occurred: {str(e)}")

        end_time = time.time()
        logger.info(f'Total execution time: {end_time - start_time} seconds')

    asyncio.run(main())
