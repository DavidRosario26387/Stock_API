import psycopg2
from pathlib import Path
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

DB_URI = os.getenv("DB_URI")

CSV_PATH = Path(r"C:\\Users\\admin\\Desktop\\Stock API\\dataset\\us_income_annual.csv")


def main():
    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
    SELECT
        *
    FROM us_derived_metrics_annual
    WHERE ticker = 'NFLX'
    order by fiscal_year desc;
    """)

    rows = cur.fetchall()
    for i in rows:
        print(i)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()