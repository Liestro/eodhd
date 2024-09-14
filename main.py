import eodhd_api_operations as api
import db_operations as db


def main():
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


if __name__ == "__main__":
    main()