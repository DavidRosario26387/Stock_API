from fastapi import APIRouter
from app.services.metrics_service import get_metrics

router = APIRouter()

@router.get("/stocks/{ticker}/metrics")
def get_stock_metrics(ticker: str, year: int | None = None):
    return get_metrics(ticker, year)
