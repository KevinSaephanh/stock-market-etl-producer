from fastapi import FastAPI
from logger import logger
from stocks import router
from stocks.stock_producer import shutdown_producer

app = FastAPI()

app.include_router(router)


@app.on_event("startup")
async def startup_event():
    """Log that the application has started"""
    logger.info("FastAPI application has started")


@app.get("/health")
async def health():
    """Checks health of application"""
    return {"status": "ok"}


@app.on_event("shutdown")
def shutdown_event():
    """Flush and close the producer when the app shuts down."""
    shutdown_producer()


