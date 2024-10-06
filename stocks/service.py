import json
from datetime import datetime, timedelta
from enum import Enum
import requests
from stock_producer import publish_stock_data
from logger import logger
from config.config import settings


class Timeframe(Enum):
    """Timeframes supported by Alphavantage"""

    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


async def get_stock_historical_data(symbol: str, timeframe: Timeframe):
    # Fetch historical data from Alphavantage
    logger.info("Fetching stock data from external API: Alphavantage")
    url = f"{settings.ALPHAVANTAGE_API_URL}/query?function=TIME_SERIES_{Timeframe[timeframe]}_ADJUSTED&symbol={symbol}&apikey={settings.ALPHAVANTAGE_API_KEY}"
    res = requests.get(url)
    data = json.loads(res.json())

    # Parse appropriate data
    logger.info("Starting process: parse stock data")
    timeframe_data = data[f"{Timeframe[timeframe]} Adjusted Time Series"]
    parsed_data = parse_stock_data(timeframe_data)

    return publish_stock_data(parsed_data)


def parse_stock_data(data: any):
    """Parse date from data keys and filter by 1 year cutoff date"""
    cutoff_date = (datetime.now() - timedelta(days=365)).date()
    parsed_data = {}

    # Iterate data items
    for date_str, values in data.items():
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        if date >= cutoff_date:
            transformed_data = {}
            # Add key/value of each field in the specific date to the map
            for key, value in values.items():
                new_key = key.split(". ", 1)[1]
                # Convert value to int (for volume) or float (for prices)
                if new_key == "volume":
                    transformed_data[new_key] = int(value)
                else:
                    transformed_data[new_key] = round(float(value), 2)
            parsed_data[date_str] = transformed_data
    return parsed_data
