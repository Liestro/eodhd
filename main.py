from async_eodhd_api import EodhdAPISession
import db_operations as db
import env_var
import asyncio

def old_main():
    # Инициализация клиентов
    eodhd_api = api.get_eodhd_api_client()
    mongo_client = db.get_mongo_client()

    # Тест подключения к MongoDB
    db.test_mongo_connection(mongo_client)

    # Получение и вывод исторических данных
    historical_data = api.get_historical_data(eodhd_api, "TSLA", "d", "2023-10-1", "2024-09-01")
    print(historical_data)

    # Получение и запись фундаментальных данных
    fundamental_data = api.get_fundamentals_data(eodhd_api, "MSFT.US")
    db.store_fundamentals_data(mongo_client, fundamental_data)

    # Получение и запись новостей
    news_data = api.get_financial_news(eodhd_api, "TSLA.US", "2023-09-01", "2024-09-02", 100)
    db.store_news_data(mongo_client, news_data)


async def main():
    print("Main started")

    # Подключение к базе
    mongo_client = db.get_mongo_client()
    db.test_mongo_connection(mongo_client)

    eodhd_api_token = env_var.EODHD_API_TOKEN
    async with EodhdAPISession(eodhd_api_token) as session:
        # symbols = await session.get_exchange_symbols("NYSE")
        symbols = ['TSLA', 'AAPL', 'MSFT']
        historical_data_tasks = [session.get_historical_data(symbol) for symbol in symbols]
        historical_data = await asyncio.gather(*historical_data_tasks)
        for symbol, data in historical_data:
            mongo_client.historical_data[symbol].create_index([("date", db.pymongo.ASCENDING)], unique=True)
            mongo_client.historical_data[symbol].insert_many(data)


if __name__ == "__main__":
    asyncio.run(main())