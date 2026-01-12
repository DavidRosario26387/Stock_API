import psycopg2
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

DB_URI = os.getenv("DB_URI")

CSV_PATH = Path(r"C:\Users\admin\Desktop\Stock API\dataset\us_balance_annual.csv")


def main():
    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor()

    # Drop tables if exist
    cur.execute("DROP TABLE IF EXISTS us_balance_annual_stage;")
    cur.execute("DROP TABLE IF EXISTS us_balance_annual;")
    conn.commit()
    print("Dropped old tables")

    # Create staging table (ticker nullable)
    cur.execute("""
    CREATE TABLE us_balance_annual_stage (
        ticker TEXT,
        simfinid INTEGER,
        fiscal_year INTEGER,
        cash_and_st_investments DOUBLE PRECISION,
        total_current_assets DOUBLE PRECISION,
        total_current_liabilities DOUBLE PRECISION,
        short_term_debt DOUBLE PRECISION,
        long_term_debt DOUBLE PRECISION,
        total_assets DOUBLE PRECISION,
        total_liabilities DOUBLE PRECISION,
        total_equity DOUBLE PRECISION,
        property_plant_equipment DOUBLE PRECISION,
        retained_earnings DOUBLE PRECISION
    );
    """)
    conn.commit()
    print("Created staging table")

    # COPY CSV into staging
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY us_balance_annual_stage
            FROM STDIN
            WITH CSV HEADER
            """,
            f
        )

    conn.commit()
    print("CSV loaded into staging")

    # Create final table (ticker, fiscal_year NOT NULL)
    cur.execute("""
    CREATE TABLE us_balance_annual (
        ticker TEXT NOT NULL,
        simfinid INTEGER,
        fiscal_year INTEGER NOT NULL,
        cash_and_st_investments DOUBLE PRECISION,
        total_current_assets DOUBLE PRECISION,
        total_current_liabilities DOUBLE PRECISION,
        short_term_debt DOUBLE PRECISION,
        long_term_debt DOUBLE PRECISION,
        total_assets DOUBLE PRECISION,
        total_liabilities DOUBLE PRECISION,
        total_equity DOUBLE PRECISION,
        property_plant_equipment DOUBLE PRECISION,
        retained_earnings DOUBLE PRECISION,
        PRIMARY KEY (ticker,fiscal_year)
    );
    """)
    conn.commit()
    print("Created final table")

    # Insert only valid rows
    cur.execute("""
    INSERT INTO us_balance_annual (
        ticker,
        simfinid,
        fiscal_year,
        cash_and_st_investments,
        total_current_assets,
        total_current_liabilities,
        short_term_debt,
        long_term_debt,
        total_assets,
        total_liabilities,
        total_equity,
        property_plant_equipment,
        retained_earnings
    )
    SELECT
        ticker,
        simfinid,
        fiscal_year,
        cash_and_st_investments,
        total_current_assets,
        total_current_liabilities,
        short_term_debt,
        long_term_debt,
        total_assets,
        total_liabilities,
        total_equity,
        property_plant_equipment,
        retained_earnings
    FROM us_balance_annual_stage
    WHERE ticker IS NOT NULL
      AND ticker <> '';
    """)
    conn.commit()
    print("Inserted valid rows")

    # Verify
    cur.execute("SELECT COUNT(*) FROM us_balance_annual;")
    print("Final row count:", cur.fetchone()[0])

    # dropped rows
    cur.execute("""
    SELECT COUNT(*) FROM us_balance_annual_stage
    WHERE ticker IS NULL OR ticker = '';
    """)
    print("Rows dropped (missing ticker):", cur.fetchone()[0])

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
