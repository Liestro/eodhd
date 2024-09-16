import asyncio
from aiohttp import ClientSession
import time

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

    async def get_historical_data(self, symbol: str):
        async with self.session.get(f'/api/eod/{symbol}?period=d&api_token={self.api_key}&fmt=json') as resp:
            return (symbol, await resp.json())

    async def get_fundamental_data(self, symbol: str):
        print(f"collecting fundamental data started for symbol {symbol}")
        await asyncio.sleep(1)
        print(f"collecting fundamental data finished for symbol {symbol}")

    async def get_news_data(self, symbol: str):
        print(f"collecting news data started for symbol {symbol}")
        await asyncio.sleep(2)
        print(f"collecting news data finished for symbol {symbol}")

if __name__ == '__main__':
    async def main():
        start_time = time.time()
        api_key = "demo"
        async with EodhdAPISession(api_key) as api:
            await asyncio.gather(
                api.get_exchange_symbols('NYSE'),
                api.get_historical_data('TSLA'),
                api.get_fundamental_data('BTC-USD'),
                api.get_news_data('BTC-USD'),
            )
        end_time = time.time()
        print(f'Total time taken: {end_time - start_time} seconds')


    asyncio.run(main())
