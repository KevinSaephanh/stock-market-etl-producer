import uvicorn
from fastapi import FastAPI
from logger import logger
from stocks import router
from .config.config import settings

app = FastAPI()

app.include_router(router)


@app.get("/health")
async def health():
    """Checks health of application"""
    return {"health": 200}


if __name__ == "__main__":
    logger.info("Starting up")
    uvicorn.run(
        app,
        port=settings.PORT,
        reload=settings.RELOAD,
        factory=True,
    )
