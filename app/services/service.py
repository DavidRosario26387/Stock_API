from fastapi import HTTPException
from app.repositories.repo import fetch_metrics,fetch_company,fetch_income,fetch_balance,fetch_bulk

def get_company(q,limit):
    q=q.strip()
    if len(q)<2:
        return []
    return fetch_company(q,limit)

def get_metrics(ticker: str, year: int | None = None):
    rows = fetch_metrics(ticker, year)

    if not rows:
        raise HTTPException(status_code=404, detail="Data not found")

    # Future business logic goes here:
    # - filtering
    # - aggregation
    # - scoring
    # - investor rules

    return rows

def get_income(ticker: str, year: int | None = None):
    rows = fetch_income(ticker, year)
    if not rows:
        raise HTTPException(status_code=404, detail="Data not found")
    return rows

def get_balance(ticker: str, year: int | None = None):
    rows = fetch_balance(ticker, year)
    if not rows:
        raise HTTPException(status_code=404, detail="Data not found")
    return rows

def get_bulk(ticker: str, year: int | None = None):
    rows = fetch_bulk(ticker, year)
    if not rows:
        raise HTTPException(status_code=404, detail="Data not found")
    return rows




