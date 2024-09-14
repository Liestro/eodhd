from eodhd import APIClient
import env_var


def get_eodhd_api_client():
    return APIClient(env_var.EODHD_API_TOKEN)


def get_historical_data(eodhd_api, symbol, interval, start_date, end_date):
    return eodhd_api.get_historical_data(symbol, interval, start_date, end_date)


def get_fundamentals_data(eodhd_api, symbol):
    return eodhd_api.get_fundamentals_data(symbol)


def get_financial_news(eodhd_api, symbol, from_date, to_date, limit):
    return eodhd_api.financial_news(s=symbol, from_date=from_date, to_date=to_date, limit=limit)