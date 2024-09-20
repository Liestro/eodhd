import asyncio
from collections import Counter
from async_eodhd_api import EodhdAPISession

async def main():
    api_key = 'demo'
    symbol = 'AAPL'

    async with EodhdAPISession(api_key) as api:
        # Получаем данные о новостях
        _, news_data = await api.get_news_data(symbol)

        if not news_data:
            print("Не удалось получить данные о новостях.")
            return

        print(f"Общее количество новостных элементов: {len(news_data)}")
        print("\nПервые 5 новостных элементов:")
        for item in news_data[:5]:
            print(item)
        print()

        # Анализ повторяющихся элементов
        news_str = [str(item) for item in news_data]
        counter = Counter(news_str)
        duplicates = {item: count for item, count in counter.items() if count > 1}

        print(f"Количество повторяющихся новостных элементов: {len(duplicates)}")
        
        if duplicates:
            print("Повторяющиеся элементы и их количество:")
            for item, count in duplicates.items():
                print(f"{item}: {count} раз")
        else:
            print("Повторяющихся новостных элементов не найдено.")

        # Анализ элементов с повторяющимися датами
        dates = [item.get('date') for item in news_data if 'date' in item]
        date_counter = Counter(dates)
        duplicate_dates = {date: count for date, count in date_counter.items() if count > 1}

        print(f"\nКоличество дат, которые повторяются: {len(duplicate_dates)}")
        print(f"Общее количество новостных элементов с повторяющимися датами: {sum(duplicate_dates.values())}")

        if duplicate_dates:
            print("Повторяющиеся даты и количество новостных элементов для каждой:")
            for date, count in duplicate_dates.items():
                print(f"{date}: {count} элементов")
        else:
            print("Новостных элементов с повторяющимися датами не найдено.")

if __name__ == '__main__':
    asyncio.run(main())