from contextlib import asynccontextmanager
from fastapi import FastAPI
from logger import logger
from stocks import router
from stocks.stock_producer import shutdown_producer


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("App started")
    yield
    shutdown_producer()


app = FastAPI(lifespan=lifespan)
app.include_router(router)


@app.get("/health")
async def health():
    return {"status": "ok"}
