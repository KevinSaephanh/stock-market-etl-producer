import uvicorn
from fastapi import FastAPI
from logger import logger
from stocks import router
from .config.config import settings
from .stocks.stock_producer import shutdown_producer

app = FastAPI()

app.include_router(router)


@app.get("/health")
async def health():
    """Checks health of application"""
    return {"health": 200}


@app.on_event("shutdown")
def shutdown_event():
    """Flush and close the producer when the app shuts down."""
    shutdown_producer()


if __name__ == "__main__":
    logger.info("Starting up")
    uvicorn.run(
        app,
        port=settings.PORT,
        reload=settings.RELOAD,
        factory=True,
    )
