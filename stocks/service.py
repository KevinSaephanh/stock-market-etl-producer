import json
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
    params = {
        "function": f"TIME_SERIES_{Timeframe[timeframe]}",
        "symbol": symbol,
        "apikey": settings.ALPHAVANTAGE_API_KEY,
    }
    res = requests.get(settings.ALPHAVANTAGE_API_URL, params=params)
    data = json.loads(res.json())
    logger.info("Fetched stock data from Alphavantage")
    return publish_stock_data(symbol, data)
