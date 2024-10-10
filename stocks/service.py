import aiohttp
from enum import Enum
from stock_producer import StockProducer
from logger import logger
from config.config import settings


class Timeframe(str, Enum):
    """Timeframes supported by Alphavantage"""

    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


class StockService:
    def __init__(self):
        self.stock_producer = StockProducer()

    async def run_etl_for_stock(self, symbol: str, timeframe: Timeframe):
        params = {
            "function": f"TIME_SERIES_{Timeframe[timeframe]}",
            "symbol": symbol,
            "apikey": settings.ALPHAVANTAGE_API_KEY,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(settings.ALPHAVANTAGE_API_URL, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Fetched stock data for {symbol} from Alphavantage")
                        return self.stock_producer.publish_stock_data(symbol, data)
                    else:
                        error_message = f"Failed to fetch data for {symbol}"
                        logger.error(error_message)
                        return {"status": response.status, "message": error_message}
        except Exception as e:
            error_message = f"Error while fetching stock data for {symbol}: {e}"
            logger.error(error_message)
            return {"status": 500, "message": error_message}
        finally:
            self.stock_producer.close()
