import logging
from pymongo import MongoClient, UpdateOne
import pymongo

# Настройка базовой конфигурации логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EodhdMongoClient(MongoClient):
    """
    MongoDB client for working with EODHD data.
    Inherits from MongoClient and provides additional methods
    for working with historical, news, and fundamental data.
    """

    def __init__(self, mongo_uri):
        """
        Initialize the EodhdMongoClient.

        :param mongo_uri: MongoDB connection URI
        """
        super().__init__(mongo_uri)

    def test_connection(self):
        """
        Tests the connection to MongoDB.
        """
        try:
            self.admin.command('ping')
            logger.info("Successfully connected to MongoDB!")
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")

    def store_historical_data(self, symbol, data):
        """
        Stores historical data for the specified symbol.
        
        :param symbol: Stock symbol (ticker)
        :param data: List of dictionaries with historical data
        """
        if data:
            self.historical_data[symbol].create_index([("date", pymongo.ASCENDING)], unique=True)
            operations = [
                UpdateOne({"date": item["date"]}, {"$set": item}, upsert=True)
                for item in data
            ]
            self.historical_data[symbol].bulk_write(operations)

    def store_news_data(self, symbol, data):
        """
        Stores news data for the specified symbol.
        
        :param symbol: Stock symbol (ticker)
        :param data: List of dictionaries with news data
        """
        if data:
            self.news_data[symbol].create_index([("date", pymongo.ASCENDING)], unique=True)
            operations = [
                UpdateOne({"date": item["date"]}, {"$set": item}, upsert=True)
                for item in data
            ]
            self.news_data[symbol].bulk_write(operations)

    def store_fundamental_data(self, symbol, data):
        """
        Stores fundamental data for the specified symbol.
        
        :param symbol: Stock symbol (ticker)
        :param data: Dictionary with fundamental data
        """
        if data:
            self.fundamental_data[symbol].replace_one({}, data, upsert=True)