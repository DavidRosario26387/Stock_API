###############################################################################
# NOT NEEDED AS INDEX COLUMN IS ALREADY INCLUDED AS PRIMARY KEY (COMPSITE KEY)#
###############################################################################

import psycopg2
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

DB_URI = os.getenv("DB_URI")


def main():
    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor()
    # cur.execute("EXPLAIN ANALYZE SELECT * FROM us_derived_metrics_annual WHERE ticker = 'AAPL';")
    # print(cur.fetchall())

    # cur.execute("CREATE INDEX IF NOT EXISTS idx_companies ON us_companies (ticker);")
    # cur.execute("CREATE INDEX IF NOT EXISTS idx_income_ticker_year ON income_annual (ticker, fiscal_year);")
    # cur.execute("CREATE INDEX IF NOT EXISTS idx_balance_ticker_year ON us_balance_annual (ticker, fiscal_year);")
    # cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_ticker_year ON us_derived_metrics_annual (ticker, fiscal_year);")
    
    # conn.commit()
    # print("created indexes")

    cur.execute('''
        CREATE OR REPLACE VIEW us_financials_annual AS
        SELECT
        i.ticker, i.fiscal_year, i.revenue, i.cost_of_revenue, i.gross_profit,
        i.operating_expenses, i.operating_income, i.net_interest_expense,
        i.net_income_tax, i.net_income, i.common_net_income,
        i.basic_shares, i.diluted_shares, i.research_development,
        i.depreciation_amortization,

        b.cash_and_st_investments, b.total_current_assets,
        b.total_current_liabilities, b.short_term_debt,
        b.long_term_debt, b.total_assets, b.total_liabilities,
        b.total_equity, b.property_plant_equipment, b.retained_earnings,
                
        m.ebitda, m.roe, m.roa, m.roic, m.gross_margin, m.operating_margin,
        m.net_margin, m.current_ratio, m.total_debt, m.net_debt_to_ebitda,
        m.eps_basic, m.eps_diluted

        FROM income_annual i
        JOIN us_balance_annual b
        ON i.ticker = b.ticker AND i.fiscal_year = b.fiscal_year
        JOIN us_derived_metrics_annual m
        ON i.ticker = m.ticker AND i.fiscal_year = m.fiscal_year;
    ''')
    conn.commit()
    print("Created view")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()



