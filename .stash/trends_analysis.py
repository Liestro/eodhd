import asyncio
from collections import Counter
from async_eodhd_api import EodhdAPISession

async def main():
    api_key = 'demo'
    symbol = 'AAPL'

    async with EodhdAPISession(api_key) as api:
        # Получаем данные о трендах
        trends_data = await api.get_trends_data(symbols=[symbol])

        if not trends_data or 'trends' not in trends_data:
            print("Не удалось получить данные о трендах.")
            return

        trends = trends_data['trends']
        if not trends or not isinstance(trends[0], list):
            print("Неожиданная структура данных трендов.")
            return

        # Объединяем все тренды в один список
        all_trends = [item for sublist in trends for item in sublist]

        print(f"Общее количество элементов в данных трендов: {len(all_trends)}")
        print("\nПервые 5 элементов данных трендов:")
        for item in all_trends[:5]:
            print(item)
        print()

        # Анализ повторяющихся элементов
        trends_str = [str(item) for item in all_trends]
        counter = Counter(trends_str)
        duplicates = {item: count for item, count in counter.items() if count > 1}

        print(f"Количество повторяющихся элементов в данных трендов: {len(duplicates)}")
        
        if duplicates:
            print("Повторяющиеся элементы и их количество:")
            for item, count in duplicates.items():
                print(f"{item}: {count} раз")
        else:
            print("Повторяющихся элементов не найдено.")

        # Анализ элементов с повторяющимися датами
        dates = [item.get('date') for item in all_trends if 'date' in item]
        date_counter = Counter(dates)
        duplicate_dates = {date: count for date, count in date_counter.items() if count > 1}

        print(f"\nКоличество дат, которые повторяются: {len(duplicate_dates)}")
        print(f"Общее количество элементов с повторяющимися датами: {sum(duplicate_dates.values())}")

        if duplicate_dates:
            print("Повторяющиеся даты и количество элементов для каждой:")
            for date, count in duplicate_dates.items():
                print(f"{date}: {count} элементов")
        else:
            print("Элементов с повторяющимися датами не найдено.")

if __name__ == '__main__':
    asyncio.run(main())