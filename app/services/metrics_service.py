from fastapi import HTTPException
from app.repositories.metrics_repo import fetch_metrics

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
