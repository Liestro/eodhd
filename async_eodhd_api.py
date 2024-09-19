import asyncio
from aiohttp import ClientSession
import time
import functools

def async_timer_decorator(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        print(f"Starting function: {func.__name__}")
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        print(f"Finished function: {func.__name__}")
        print(f"Execution time: {end_time - start_time:.4f} seconds")
        return result
    return wrapper

class EodhdAPISession:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = ClientSession(base_url='https://eodhd.com')

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.session.close()

    async def get_exchange_symbols(self, exchange: str):
        async with self.session.get(f'/api/exchange-symbol-list/{exchange}?api_token={self.api_key}&fmt=json') as resp:
            response_json = await resp.json()
            codes = [item['Code'] for item in response_json]
            return codes

    @async_timer_decorator
    async def get_historical_data(self, symbol: str):
        async with self.session.get(f'/api/eod/{symbol}?period=d&api_token={self.api_key}&fmt=json') as resp:
            if resp.status == 200:
                historical_data = await resp.json()
                print(f"Received historical data for symbol {symbol}")
                return (symbol, historical_data)
            else:
                print(f"Error getting historical data for symbol {symbol}: {resp.status}")
                return (symbol, None)

    @async_timer_decorator
    async def get_fundamental_data(self, symbol: str):
        async with self.session.get(f'/api/fundamentals/{symbol}?api_token={self.api_key}&fmt=json') as resp:
            if resp.status == 200:
                fundamental_data = await resp.json()
                print(f"Received fundamental data for symbol {symbol}")
                return (symbol, fundamental_data)
            else:
                print(f"Error getting fundamental data for symbol {symbol}: {resp.status}")
                return (symbol, None)

    @async_timer_decorator
    async def get_news_data(self, symbol: str):
        async with self.session.get(f'/api/news?s={symbol}&api_token={self.api_key}&fmt=json') as resp:
            if resp.status == 200:
                news_data = await resp.json()
                print(f"Received {len(news_data)} news articles for symbol {symbol}")
                return (symbol, news_data)
            else:
                print(f"Error getting news data for symbol {symbol}: {resp.status}")
                return (symbol, None)

if __name__ == '__main__':
    async def main():
        start_time = time.time()
        api_key = "demo"
        async with EodhdAPISession(api_key) as api:
            results = await asyncio.gather(
                api.get_historical_data('TSLA'),
                api.get_fundamental_data('TSLA'),
                api.get_news_data('TSLA'),
                # api.get_exchange_symbols('NYSE'),
            )
            
            # Output results
            print(f"TSLA historical data: {len(results[0][1]) if results[0][1] else 'No data'} records")
            print(f"TSLA fundamental data: {'Received' if results[1][1] else 'No data'}")
            print(f"TSLA news data: {len(results[2][1]) if results[2][1] else 'No data'} articles")
            if results[2][1]:
                print(results[2][1][0].keys())
            # print(f"NYSE symbols: {len(results[3])} received")

        end_time = time.time()
        print(f'Total execution time: {end_time - start_time} seconds')

    asyncio.run(main())
