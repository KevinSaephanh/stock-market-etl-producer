from fastapi import APIRouter, HTTPException
from service import Timeframe, get_stock_historical_data
from logger import logger


router = APIRouter(tags=["Stocks"])


@router.post("/{symbol}/{timeframe}")
def run_etl(symbol: str, timeframe: Timeframe) -> any:
    """Fetch historical data for stock ticker symbol and publish to Kafka"""
    try:
        return get_stock_historical_data(symbol, timeframe)
    except HTTPException as e:
        logger.error("Error occurred: %s", e)
        raise e
