import asyncio
from fastapi import APIRouter, HTTPException
from service import Timeframe, StockService
from logger import logger


router = APIRouter(tags=["Stocks"])
stock_service = StockService()


@router.post("/stock-data/{symbol}/{timeframe}")
def run_etl(symbol: str, timeframe: Timeframe):
    try:
        return stock_service.run_etl_for_stock(symbol, timeframe)
    except HTTPException as e:
        logger.error("Error occurred: %s", e)
        raise e


@router.post("/bulk-etl")
async def run_bulk_etl(symbols: list):
    if len(list) > 5:
        return {"status": 400, "message": "Stock querying limited to 5 symbols"}
    try:
        # Create a list of tasks for each symbol and timeframe
        tasks = [stock_service.run_etl_for_stock(sym, t) for sym in symbols for t in Timeframe]
        # Run tasks concurrently
        await asyncio.gather(*tasks)
        return {
            "status": 200,
            "message": f"Successfully published the following symbols to Kafka: {symbols}",
        }
    except HTTPException as e:
        logger.error("Error occurred: %s", e)
        raise e
