from fastapi import APIRouter, HTTPException
from service import StockRequest, run_etl_for_stocks
from logger import logger


router = APIRouter(tags=["Stocks"])


@router.post("/bulk-etl")
async def run_bulk_etl(req: StockRequest) -> any:
    """Fetch historical data for list of stocks and publish to Kafka"""
    if len(list) > 5:
        return {"status": 400, "message": "Stock querying limited to 5 symbols"}
    try:
        run_etl_for_stocks(req)
        return {
            "status": 200,
            "message": f"Successfully published symbols: {req.symbols} to Kafka",
        }
    except HTTPException as e:
        logger.error("Error occurred: %s", e)
        raise e
