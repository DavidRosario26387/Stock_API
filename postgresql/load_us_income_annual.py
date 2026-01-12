import psycopg2
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

DB_URI = os.getenv("DB_URI")

CSV_PATH = Path(r"C:\Users\admin\Desktop\Stock API\dataset\us_income_annual.csv")


def main():
    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor()

    # Drop tables if exist
    cur.execute("DROP TABLE IF EXISTS income_annual_stage;")
    cur.execute("DROP TABLE IF EXISTS income_annual;")
    conn.commit()
    print("Dropped old tables")

    # Create staging table (ticker nullable)
    cur.execute("""
    CREATE TABLE income_annual_stage (
        ticker TEXT,
        simfinid INTEGER,
        fiscal_year INTEGER,

        revenue DOUBLE PRECISION,
        cost_of_revenue DOUBLE PRECISION,
        gross_profit DOUBLE PRECISION,
        operating_expenses DOUBLE PRECISION,
        operating_income DOUBLE PRECISION,
        net_interest_expense DOUBLE PRECISION,
        net_income_tax DOUBLE PRECISION,
        net_income DOUBLE PRECISION,
        common_net_income DOUBLE PRECISION,

        basic_shares DOUBLE PRECISION,
        diluted_shares DOUBLE PRECISION,
        research_development DOUBLE PRECISION,
        depreciation_amortization DOUBLE PRECISION
    );
    """)
    conn.commit()
    print("Created staging table")

    # COPY CSV into staging
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY income_annual_stage
            FROM STDIN
            WITH CSV HEADER
            """,
            f
        )

    conn.commit()
    print("CSV loaded into staging")

    # Create final table (ticker NOT NULL)
    cur.execute("""
    CREATE TABLE income_annual (
        ticker TEXT NOT NULL,
        simfinid INTEGER,
        fiscal_year INTEGER NOT NULL,

        revenue DOUBLE PRECISION,
        cost_of_revenue DOUBLE PRECISION,
        gross_profit DOUBLE PRECISION,
        operating_expenses DOUBLE PRECISION,
        operating_income DOUBLE PRECISION,
        net_interest_expense DOUBLE PRECISION,
        net_income_tax DOUBLE PRECISION,
        net_income DOUBLE PRECISION,
        common_net_income DOUBLE PRECISION,

        basic_shares DOUBLE PRECISION,
        diluted_shares DOUBLE PRECISION,
        research_development DOUBLE PRECISION,
        depreciation_amortization DOUBLE PRECISION,

        PRIMARY KEY (ticker, fiscal_year)
    );
    """)
    conn.commit()
    print("Created final table")

    # Insert only valid rows
    cur.execute("""
    INSERT INTO income_annual
    SELECT *
    FROM income_annual_stage
    WHERE ticker IS NOT NULL
      AND ticker <> '';
    """)
    conn.commit()
    print("Inserted valid rows into final table")

    # Verify
    cur.execute("SELECT COUNT(*) FROM income_annual;")
    print("Final row count:", cur.fetchone()[0])

    # dropped rows
    cur.execute("""
    SELECT COUNT(*) FROM income_annual_stage
    WHERE ticker IS NULL OR ticker = '';
    """)
    print("Rows dropped (missing ticker):", cur.fetchone()[0])

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
