import pandas as pd
import numpy as np

# Load data
income_df = pd.read_csv(
    r"C:\Users\admin\Desktop\Stock API\dataset\us_income_annual.csv"
)
balance_df = pd.read_csv(
    r"C:\Users\admin\Desktop\Stock API\dataset\us_balance_annual.csv"
)

# Merge
df = income_df.merge(
    balance_df,
    on=["Ticker", "Fiscal_Year"],
    how="inner"
)

# Safety division
def safe_div(n, d):
    return np.where(d == 0, np.nan, n / d)

# ---------- CORE CALCULATIONS ----------

# EBITDA
df["EBITDA"] = (
    df["Operating_Income"] +
    df["Depreciation_Amortization"].fillna(0)
)

# Debt
df["Total_Debt"] = (
    df["Short_Term_Debt"].fillna(0) +
    df["Long_Term_Debt"].fillna(0)
)

# Returns
df["ROE"] = safe_div(df["Common_Net_Income"], df["Total_Equity"])
df["ROA"] = safe_div(df["Common_Net_Income"], df["Total_Assets"])
df["ROIC"] = safe_div(
    df["Operating_Income"],
    df["Total_Equity"] + df["Total_Debt"]
)

# ---------- MARGINS ----------
df["Gross_Margin"] = safe_div(df["Gross_Profit"], df["Revenue"])
df["Operating_Margin"] = safe_div(df["Operating_Income"], df["Revenue"])
df["Net_Margin"] = safe_div(df["Common_Net_Income"], df["Revenue"])

# ---------- LIQUIDITY ----------
df["Current_Ratio"] = safe_div(
    df["Total_Current_Assets"],
    df["Total_Current_Liabilities"]
)

# ---------- LEVERAGE ----------
df["Net_Debt"] = (
    df["Total_Debt"] -
    df["Cash_and_ST_Investments"].fillna(0)
)

df["Net_Debt_to_EBITDA"] = safe_div(
    df["Net_Debt"],
    df["EBITDA"]
)

# ---------- PER SHARE ----------
df["EPS_Basic"] = safe_div(
    df["Common_Net_Income"],
    df["Basic_Shares"]
)

df["EPS_Diluted"] = safe_div(
    df["Common_Net_Income"],
    df["Diluted_Shares"]
)

# ---------- FINAL DERIVED TABLE ----------
derived_df = df[
    [
        "Ticker",
        "Fiscal_Year",
        "EBITDA",
        "ROE",
        "ROA",
        "ROIC",
        "Gross_Margin",
        "Operating_Margin",
        "Net_Margin",
        "Current_Ratio",
        "Total_Debt",
        "Net_Debt_to_EBITDA",
        "EPS_Basic",
        "EPS_Diluted",
    ]
]

# Save
derived_df.to_csv(
    "derived_metrics_annual.csv",
    index=False
)

print("Derived metrics created successfully.")
