from fastapi import APIRouter,Query
from app.services.service import get_metrics,get_company

router = APIRouter()

@router.get("/stocks/{ticker}/metrics")
def get_stock_metrics(ticker: str, year: int | None = None):
    return get_metrics(ticker, year)

@router.get("/companies")
def search_company(q: str = Query(..., min_length=2),limit: int = Query(4, le=10)):
    return get_company(q, limit)
