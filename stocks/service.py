import json
from datetime import datetime, timedelta
from enum import Enum
import requests
from stock_producer import publish_stock_data
from logger import logger
from config.config import settings


class Timeframe(str, Enum):
    """Timeframes supported by Alphavantage"""

    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


async def run_etl_for_stock(symbol: str, timeframe: Timeframe):
    # Fetch historical data from Alphavantage
    logger.info("Fetching stock data from external API: Alphavantage")
    url = f"{settings.ALPHAVANTAGE_API_URL}/query?function=TIME_SERIES_{Timeframe[timeframe]}_ADJUSTED&symbol={symbol}&apikey={settings.ALPHAVANTAGE_API_KEY}"
    res = requests.get(url)
    data = json.loads(res.json())

    # Parse appropriate data
    logger.info("Starting process: parse stock data")
    parsed_data = parse_stock_data(symbol, data, Timeframe[timeframe])

    return publish_stock_data(parsed_data)


def parse_stock_data(symbol: str, data: any, timeframe: str):
    """Parse date from data keys and filter by 1 year cutoff date"""
    timeframe_data = data[f"{timeframe.title()} Adjusted Time Series"]
    cutoff_date = (datetime.now() - timedelta(days=365)).date()
    parsed_data = {"symbol": symbol, "timeframe": timeframe}

    # Iterate data items
    for date_str, values in timeframe_data.items():
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        if date >= cutoff_date:
            transformed_data = {}
            # Add key/value of each field in the specific date to the map
            for key, value in values.items():
                # Parse name from key
                new_key = key.split(". ", 1)[1]
                if new_key == "adjusted close":
                    new_key = "adjusted_close"
                elif new_key == "dividend amount":
                    new_key = "dividend"

                # Convert value to int (for volume) or float (for prices)
                if new_key == "volume":
                    transformed_data[new_key] = int(value)
                else:
                    transformed_data[new_key] = round(float(value), 2)
            parsed_data[date_str] = transformed_data
    return parsed_data
