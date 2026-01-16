from psycopg2.extras import RealDictCursor
from app.db import get_connection, release_connection

def fetch_company(q,limit):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    sql = """
    SELECT ticker, company_name, isin, cik, market
    FROM us_companies
    WHERE
        ticker ILIKE %(prefix)s
        OR isin ILIKE %(prefix)s
        OR cik ILIKE %(prefix)s
        OR company_name ILIKE %(contains)s
    ORDER BY
        CASE
            WHEN ticker = %(exact)s THEN 1
            WHEN isin   = %(exact)s THEN 2
            WHEN cik    = %(exact)s THEN 3
            WHEN ticker ILIKE %(prefix)s THEN 4
            WHEN company_name   ILIKE %(prefix)s THEN 5
            ELSE 6
        END
    LIMIT %(limit)s;
    """
    q_norm = q.strip()
    q_upper = q_norm.upper()
    params = {
        "exact": q_upper,
        "prefix": f"{q_upper}%",
        "contains": f"%{q_norm}%",
        "limit": limit
    }

    try:
        cur.execute(sql, params)
        return cur.fetchall()

    finally:
        cur.close()
        release_connection(conn)

def fetch_metrics(ticker: str, year: int | None = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if year is not None:
            cur.execute("""
                SELECT *
                FROM us_derived_metrics_annual
                WHERE ticker = %s AND fiscal_year = %s
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

def fetch_income(ticker: str, year: int | None = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if year is not None:
            cur.execute("""
                SELECT *
                FROM income_annual
                WHERE ticker = %s AND fiscal_year = %s
            """, (ticker, year))
        else:
            cur.execute("""
                SELECT *
                FROM income_annual
                WHERE ticker = %s
                ORDER BY fiscal_year DESC
            """, (ticker,))

        return cur.fetchall()

    finally:
        cur.close()
        release_connection(conn)

def fetch_balance(ticker: str, year: int | None = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if year is not None:
            cur.execute("""
                SELECT *
                FROM us_balance_annual
                WHERE ticker = %s AND fiscal_year = %s
            """, (ticker, year))
        else:
            cur.execute("""
                SELECT *
                FROM us_balance_annual
                WHERE ticker = %s
                ORDER BY fiscal_year DESC
            """, (ticker,))

        return cur.fetchall()

    finally:
        cur.close()
        release_connection(conn)

def fetch_bulk(ticker: str, year: int | None = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if year is not None:
            cur.execute("""
                SELECT *
                FROM us_financials_annual
                WHERE ticker = %s AND fiscal_year = %s
            """, (ticker, year))
        else:
            cur.execute("""
                SELECT *
                FROM us_financials_annual
                WHERE ticker = %s
                ORDER BY fiscal_year DESC
            """, (ticker,))

        return cur.fetchall()

    finally:
        cur.close()
        release_connection(conn)
