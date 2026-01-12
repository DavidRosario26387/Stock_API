import psycopg2
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

DB_URI = os.getenv("DB_URI")

CSV_PATH = Path(r"C:\Users\admin\Desktop\Stock API\dataset\us_companies.csv")


def main():
    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor()

    # Drop tables if exist
    cur.execute("DROP TABLE IF EXISTS us_companies_stage;")
    cur.execute("DROP TABLE IF EXISTS us_companies;")
    conn.commit()
    print("Dropped old tables")

    # Create staging table (ticker nullable)
    cur.execute("""
    CREATE TABLE us_companies_stage (
        ticker TEXT,
        simfinid INTEGER,
        company_name TEXT,
        isin TEXT,
        number_employees INTEGER,
        business_summary TEXT,
        market TEXT,
        cik TEXT
    );
    """)
    conn.commit()
    print("Created staging table")

    # COPY CSV into staging
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY us_companies_stage
            FROM STDIN
            WITH CSV HEADER
            """,
            f
        )

    conn.commit()
    print("CSV loaded into staging")

    # Create final table (ticker NOT NULL)
    cur.execute("""
    CREATE TABLE us_companies (
        ticker TEXT NOT NULL,
        simfinid INTEGER,
        company_name TEXT,
        isin TEXT,
        number_employees INTEGER,
        business_summary TEXT,
        market TEXT,
        cik TEXT,
        PRIMARY KEY (ticker)
    );
    """)
    conn.commit()
    print("Created final table")

    # Insert only valid rows
    cur.execute("""
    INSERT INTO us_companies (
        ticker,
        simfinid,
        company_name,
        isin,
        number_employees,
        business_summary,
        market,
        cik
    )
    SELECT
        ticker,
        simfinid,
        company_name,
        isin,
        number_employees,
        business_summary,
        market,
        cik
    FROM us_companies_stage
    WHERE ticker IS NOT NULL
      AND ticker <> '';
    """)
    conn.commit()
    print("Inserted valid rows")

    # Verify
    cur.execute("SELECT COUNT(*) FROM us_companies;")
    print("Final row count:", cur.fetchone()[0])

    # dropped rows
    cur.execute("""
    SELECT COUNT(*) FROM us_companies_stage
    WHERE ticker IS NULL OR ticker = '';
    """)
    print("Rows dropped (missing ticker):", cur.fetchone()[0])

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
