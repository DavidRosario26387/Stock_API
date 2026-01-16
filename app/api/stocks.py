from fastapi import APIRouter,Query
from app.services.service import get_metrics,get_company,get_income,get_balance,get_bulk

router = APIRouter()

@router.get("/companies")
def search_company(q: str = Query(..., min_length=2),limit: int = Query(4, le=10)):
    return get_company(q, limit)

@router.get("/stocks/{ticker}/metrics")
def get_stock_metrics(ticker: str, year: int | None = None):
    return get_metrics(ticker, year)

@router.get("/stocks/{ticker}/income")
def get_stock_income(ticker: str, year: int | None = None):
    return get_income(ticker, year)

@router.get("/stocks/{ticker}/balance")
def get_stock_balance(ticker: str, year: int | None = None):
    return get_balance(ticker, year)

@router.get("/stocks/{ticker}/bulk")
def get_stock_bulk(ticker: str, year: int | None = None):
    return get_bulk(ticker, year)


