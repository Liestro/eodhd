import logging
from pymongo import MongoClient, UpdateOne, ASCENDING

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
            self.historical_data[symbol].create_index([("date", ASCENDING)], unique=True)
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
            # Create a compound index on date and title (assuming title is unique for a given date)
            self.news_data[symbol].create_index([("date", ASCENDING), ("title", ASCENDING)], unique=True)
            operations = [
                UpdateOne(
                    {"date": item["date"], "title": item["title"]},
                    {"$set": item},
                    upsert=True
                )
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

    def store_earnings_data(self, data: dict):
        """
        Stores earnings data in the database.
        
        :param data: Dictionary with earnings data
        """
        if 'earnings' not in data:
            logger.error("Invalid data format for earnings: 'earnings' key is missing")
            return

        earnings = data['earnings']

        try:
            for symbol_data in earnings:
                if not symbol_data:
                    continue
                symbol = symbol_data[0].get('code')
                if not symbol:
                    logger.warning(f"Symbol code is missing in earnings data: {symbol_data}")
                    continue

                collection = self.earnings_data[symbol]
                
                # Create a compound index on all fields of the first item
                if symbol_data:
                    index_fields = [(field, ASCENDING) for field in symbol_data[0].keys()]
                    collection.create_index(index_fields, unique=True)
                
                    operations = [
                        UpdateOne(
                            item,  # Use all fields as the filter
                            {"$set": item},
                            upsert=True
                        ) for item in symbol_data
                    ]
                    result = collection.bulk_write(operations)
                    logger.info(f"Upserted {result.upserted_count} and modified {result.modified_count} earnings records for symbol: {symbol}")
                else:
                    logger.info(f"No earnings data found for symbol: {symbol}")

            logger.info(f"Earnings data processing completed for {len(earnings)} symbols")
        except Exception as e:
            logger.error(f"Error occurred while storing earnings data: {e}")

    def store_trends_data(self, data: dict):
        """
        Stores trends data in the database.
        
        :param data: Dictionary with trends data
        """
        if 'trends' not in data:
            logger.error("Invalid data format for trends: 'trends' key is missing")
            return

        trends = data['trends']

        try:
            for symbol_data in trends:
                if not symbol_data:
                    continue
                symbol = symbol_data[0].get('code')
                if not symbol:
                    logger.warning(f"Symbol code is missing in trends data: {symbol_data}")
                    continue

                collection = self.trends_data[symbol]
                
                # Create a compound index on all fields of the first item
                if symbol_data:
                    index_fields = [(field, ASCENDING) for field in symbol_data[0].keys()]
                    collection.create_index(index_fields, unique=True)
                
                    operations = [
                        UpdateOne(
                            item,  # Use all fields as the filter
                            {"$set": item},
                            upsert=True
                        ) for item in symbol_data
                    ]
                    result = collection.bulk_write(operations)
                    logger.info(f"Upserted {result.upserted_count} and modified {result.modified_count} trends records for symbol: {symbol}")
                else:
                    logger.info(f"No trends data found for symbol: {symbol}")

            logger.info(f"Trends data processing completed for {len(trends)} symbols")
        except Exception as e:
            logger.error(f"Error occurred while storing trends data: {e}")