import psycopg2
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

DB_URI = os.getenv("DB_URI")

CSV_PATH = Path(r"C:\Users\admin\Desktop\Stock API\dataset\us_derived_metrics_annual.csv")


def main():
    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor()

    # Drop tables if exist
    cur.execute("DROP TABLE IF EXISTS us_derived_metrics_annual_stage;")
    cur.execute("DROP TABLE IF EXISTS us_derived_metrics_annual;")
    conn.commit()
    print("Dropped old tables")

    # Create staging table (ticker nullable)
    cur.execute("""
    CREATE TABLE us_derived_metrics_annual_stage (
        ticker TEXT,
        fiscal_year INTEGER,
        ebitda DOUBLE PRECISION,
        roe DOUBLE PRECISION,
        roa DOUBLE PRECISION,
        roic DOUBLE PRECISION,
        gross_margin DOUBLE PRECISION,
        operating_margin DOUBLE PRECISION,
        net_margin DOUBLE PRECISION,
        current_ratio DOUBLE PRECISION,
        total_debt DOUBLE PRECISION,
        net_debt_to_ebitda DOUBLE PRECISION,
        eps_basic DOUBLE PRECISION,
        eps_diluted DOUBLE PRECISION
    );
    """)
    conn.commit()
    print("Created staging table")

    # COPY CSV into staging
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY us_derived_metrics_annual_stage
            FROM STDIN
            WITH CSV HEADER
            """,
            f
        )

    conn.commit()
    print("CSV loaded into staging")

    # Create final table (ticker, fiscal_year NOT NULL)
    cur.execute("""
    CREATE TABLE us_derived_metrics_annual (
        ticker TEXT NOT NULL,
        fiscal_year INTEGER NOT NULL,
        ebitda DOUBLE PRECISION,
        roe DOUBLE PRECISION,
        roa DOUBLE PRECISION,
        roic DOUBLE PRECISION,
        gross_margin DOUBLE PRECISION,
        operating_margin DOUBLE PRECISION,
        net_margin DOUBLE PRECISION,
        current_ratio DOUBLE PRECISION,
        total_debt DOUBLE PRECISION,
        net_debt_to_ebitda DOUBLE PRECISION,
        eps_basic DOUBLE PRECISION,
        eps_diluted DOUBLE PRECISION,
        PRIMARY KEY (ticker,fiscal_year)
    );
    """)
    conn.commit()
    print("Created final table")

    # Insert only valid rows
    cur.execute("""
    INSERT INTO us_derived_metrics_annual (
        ticker,
        fiscal_year,
        ebitda,
        roe,
        roa,
        roic,
        gross_margin,
        operating_margin,
        net_margin,
        current_ratio,
        total_debt,
        net_debt_to_ebitda,
        eps_basic,
        eps_diluted
    )
    SELECT
        ticker,
        fiscal_year,
        ebitda,
        roe,
        roa,
        roic,
        gross_margin,
        operating_margin,
        net_margin,
        current_ratio,
        total_debt,
        net_debt_to_ebitda,
        eps_basic,
        eps_diluted
    FROM us_derived_metrics_annual_stage
    WHERE ticker IS NOT NULL
      AND ticker <> '';
    """)
    conn.commit()
    print("Inserted valid rows")

    # Verify
    cur.execute("SELECT COUNT(*) FROM us_derived_metrics_annual;")
    print("Final row count:", cur.fetchone()[0])

    # dropped rows
    cur.execute("""
    SELECT COUNT(*) FROM us_derived_metrics_annual_stage
    WHERE ticker IS NULL OR ticker = '';
    """)
    print("Rows dropped (missing ticker):", cur.fetchone()[0])

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
