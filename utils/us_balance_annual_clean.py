import pandas as pd

df=pd.read_csv('us_balance_annual.csv',sep=";")
print(df.head())

df=df[["Ticker",
"SimFinId",
"Fiscal Year",
"Cash, Cash Equivalents & Short Term Investments",
"Total Current Assets",
"Total Current Liabilities",
"Short Term Debt",
"Long Term Debt",
"Total Assets",
"Total Liabilities",
"Total Equity",
"Property, Plant & Equipment, Net",
"Retained Earnings"]]

df=df.rename(columns={
    "Fiscal Year":"Fiscal_Year",
"Cash, Cash Equivalents & Short Term Investments":"Cash_ST_Investments",
"Total Current Assets":"Total_Current_Assets",
"Total Current Liabilities":"Total_Current_Liabilities",
"Short Term Debt":"Short_Term_Debt",
"Long Term Debt":"Long_Term_Debt",
"Total Assets":"Total_Assets",
"Total Liabilities":"Total_Liabilities",
"Total Equity":"Total_Equity",
"Property, Plant & Equipment, Net":"Property_Plant_Equipment",
"Retained Earnings":"Retained_Earnings"
})

print(df.columns)
print(df.isnull().sum())
df.to_csv("us_balance_annual_fixed.csv",index=False)