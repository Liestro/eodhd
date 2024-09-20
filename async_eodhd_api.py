import asyncio
from aiohttp import ClientSession, ClientError, ClientResponseError, ClientConnectorError
import time
import functools
import json
import logging
from typing import Dict, Any, List

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

    @async_timer_decorator
    async def get_earnings_data(self, symbols: List[str] = None, from_date: str = None, to_date: str = None, fmt: str = 'json'):
        params = {}
        if from_date:
            params['from'] = from_date
        if to_date:
            params['to'] = to_date
        if symbols:
            params['symbols'] = ','.join(symbols)
        params['fmt'] = fmt
        data = await self._make_request('/api/calendar/earnings', params)
        logger.info(f"Received earnings data")
        return data

    @async_timer_decorator
    async def get_trends_data(self, symbols: List[str], fmt: str = 'json'):
        params = {'symbols': ','.join(symbols), 'fmt': fmt}
        data = await self._make_request('/api/calendar/trends', params)
        logger.info(f"Received trends data")
        return data

    @async_timer_decorator
    async def get_ipos_data(self, from_date: str = None, to_date: str = None, fmt: str = 'json'):
        params = {}
        if from_date:
            params['from'] = from_date
        if to_date:
            params['to'] = to_date
        params['fmt'] = fmt
        data = await self._make_request('/api/calendar/ipos', params)
        logger.info(f"Received IPOs data")
        return data

    @async_timer_decorator
    async def get_splits_data(self, from_date: str = None, to_date: str = None, fmt: str = 'json'):
        params = {}
        if from_date:
            params['from'] = from_date
        if to_date:
            params['to'] = to_date
        params['fmt'] = fmt
        data = await self._make_request('/api/calendar/splits', params)
        logger.info(f"Received splits data")
        return data

    @async_timer_decorator
    async def get_macro_indicators_data(self, country: str, indicator: str = None, fmt: str = 'json'):
        params = {'fmt': fmt}
        if indicator:
            params['indicator'] = indicator
        data = await self._make_request(f'/api/macro-indicator/{country}', params)
        logger.info(f"Received macro indicators data for country: {country}")
        return data

if __name__ == '__main__':
    async def main():
        start_time = time.time()
        api_key = "demo"
        async with EodhdAPISession(api_key) as api:
            try:
                symbols = ['AAPL', 'TSLA', 'MSFT']  # List of symbols
                indices = ['GSPC.INDX']
                tasks = {
                    # 'historical_data': [api.get_historical_data(symbol) for symbol in symbols],
                    # 'fundamental_data': [api.get_fundamental_data(symbol) for symbol in symbols],
                    # 'news_data': [api.get_news_data(symbol) for symbol in symbols],
                    # 'exchange_symbols': api.get_exchange_symbols('NYSE'),
                    # 'index_data': api.get_index_data(indices),  # S&P 500 index
                    # 'earnings_data': [api.get_earnings_data(symbols=symbols)],  # Get event calendar data
                    # 'trends_data': [api.get_trends_data(['AAPL'])],
                    # 'ipos_data': [api.get_ipos_data()],  # Get IPOs data
                    # 'splits_data': [api.get_splits_data()],  # Get splits data
                }

                result = dict(zip(
                    tasks.keys(),
                    await asyncio.gather(
                        *(asyncio.gather(*task_list, return_exceptions=True) for task_list in tasks.values())
                        )
                    )
                )

                logger.info(f"Combined results: {result}")

            except (ClientResponseError, ClientConnectorError, ClientError, json.JSONDecodeError, RuntimeError) as e:
                logger.error(f"Error occurred: {str(e)}")

        end_time = time.time()
        logger.info(f'Total execution time: {end_time - start_time} seconds')

    asyncio.run(main())
