from typing import List
from enum import Enum
from alpha_vantage.timeseries import TimeSeries

from pydantic import BaseModel
from stock_producer import publish_stock_data
from logger import logger
from config.config import settings

    
class Timeframe(str, Enum):
    """Timeframes supported by Alphavantage"""

    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


class StockRequest(BaseModel):
    symbols: List[str]
    timeframe: Timeframe


async def run_etl_for_stocks(req: StockRequest):
    data = None
    ts = TimeSeries(key=settings.ALPHAVANTAGE_API_KEY, output_format="compact")

    for symbol in req.symbols:
        match req.timeframe:
            case Timeframe.DAILY:
                data, *_ = ts.get_daily_adjusted(symbol=symbol)
            case Timeframe.WEEKLY:
                data, *_ = ts.get_weekly_adjusted(symbol=symbol)
            case Timeframe.MONTHLY:
                data, *_ = ts.get_monthly_adjusted(symbol=symbol)
            case _:
                logger.error(f"Error while fetching stock data for {symbol}")
                return None
        publish_stock_data(symbol, data)
    return data
