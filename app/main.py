from fastapi import FastAPI
from app.api.stocks import router as stocks_router

app = FastAPI(title="Stock Data API")

app.include_router(stocks_router)
