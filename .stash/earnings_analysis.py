import asyncio
from collections import Counter
from async_eodhd_api import EodhdAPISession

async def main():
    api_key = 'demo'
    symbol = 'AAPL'

    async with EodhdAPISession(api_key) as api:
        # Получаем данные о доходах
        earnings_data = await api.get_earnings_data(symbols=[symbol])

        # Проверяем, есть ли в ответе список 'earnings'
        if 'earnings' not in earnings_data:
            print("В полученных данных нет списка 'earnings'.")
            return

        earnings = earnings_data['earnings']

        # Анализ повторяющихся элементов
        earnings_str = [str(item) for item in earnings]
        counter = Counter(earnings_str)
        duplicates = {item: count for item, count in counter.items() if count > 1}

        print(f"Количество повторяющихся элементов в списке 'earnings': {len(duplicates)}")
        
        if duplicates:
            print("Повторяющиеся элементы и их количество:")
            for item, count in duplicates.items():
                print(f"{item}: {count} раз")
        else:
            print("Повторяющихся элементов не найдено.")

        # Анализ элементов с повторяющимися датами
        dates = [item.get('date') for item in earnings if 'date' in item]
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