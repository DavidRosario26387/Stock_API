from psycopg2.extras import RealDictCursor
from app.db import get_connection, release_connection

def fetch_metrics(ticker: str, year: int | None = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if year:
            cur.execute("""
                SELECT *
                FROM us_derived_metrics_annual
                WHERE ticker = %s AND fiscal_year = %s
                ORDER BY fiscal_year DESC
            """, (ticker, year))
        else:
            cur.execute("""
                SELECT *
                FROM us_derived_metrics_annual
                WHERE ticker = %s
                ORDER BY fiscal_year DESC
            """, (ticker,))

        return cur.fetchall()

    finally:
        cur.close()
        release_connection(conn)
