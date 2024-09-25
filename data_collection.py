from async_eodhd_api import EodhdAPISession
from db_operations import EodhdMongoClient
from typing import List

class DataCollector:
    def __init__(self, eodhd_api_token: str, mongo_uri: str):
        self.__eodhd_api_token = eodhd_api_token
        self.__mongo_uri = mongo_uri
        self.__session = EodhdAPISession(self.__eodhd_api_token)
        self.__mongo_client = EodhdMongoClient(self.__mongo_uri)

    async def __aenter__(self):
        self.__mongo_client.test_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.__session.close()
        self.__mongo_client.close()

    async def collect_and_store_historical_data(self, symbol: str):
        historical_data = await self.__session.get_historical_data(symbol)
        self.__mongo_client.store_historical_data(symbol, historical_data[1])

    async def collect_and_store_fundamental_data(self, symbol: str):
        fundamental_data = await self.__session.get_fundamental_data(symbol)
        self.__mongo_client.store_fundamental_data(symbol, fundamental_data[1])

    async def collect_and_store_news_data(self, symbol: str):
        news_data = await self.__session.get_news_data(symbol)
        self.__mongo_client.store_news_data(symbol, news_data[1])

    async def collect_and_store_indices_data(self, index: str):
        index_data = await self.__session.get_index_data(index)
        self.__mongo_client.store_historical_data(index, index_data[1])

    async def collect_and_store_earnings_data(self, symbols: List[str] = []):
        earnings_data = await self.__session.get_earnings_data(symbols=symbols)
        self.__mongo_client.store_earnings_data(earnings_data)

    async def collect_and_store_trends_data(self, symbols: List[str]):
        trends_data = await self.__session.get_trends_data(symbols)
        self.__mongo_client.store_trends_data(trends_data)

    async def collect_and_store_ipos_data(self):
        ipos_data = await self.__session.get_ipos_data()
        self.__mongo_client.store_ipos_data(ipos_data)

    async def collect_and_store_splits_data(self):
        splits_data = await self.__session.get_splits_data()
        self.__mongo_client.store_splits_data(splits_data)

    async def collect_and_store_macro_indicators_data(self, country: str):
        macro_indicators_data = await self.__session.get_macro_indicators_data(country)
        self.__mongo_client.store_macro_indicators_data(macro_indicators_data)