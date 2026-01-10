import pandas as pd

df= pd.read_csv('us_income_annual.csv',sep=";")

df=df[["Ticker","SimFinId","Fiscal Year","Revenue","Cost of Revenue","Gross Profit","Operating Expenses",
"Operating Income (Loss)","Interest Expense, Net","Income Tax (Expense) Benefit, Net","Net Income",
"Net Income (Common)","Shares (Basic)","Shares (Diluted)","Research & Development","Depreciation & Amortization"]]

df=df.rename(columns={"Fiscal Year":"Fiscal_Year","Cost of Revenue":"Cost_of_Revenue","Gross Profit":"Gross_Profit",
                      "Operating Expenses":"Operating_Expenses","Operating Income (Loss)":"Operating_Income",
                      "Interest Expense, Net":"Net_Interest_Expense","Income Tax (Expense) Benefit, Net":"Net_Income_Tax",
                      "Net Income":"Net_Income","Net Income (Common)":"Common_Net_Income",
                      "Shares (Basic)":"Basic_Shares","Shares (Diluted)":"Diluted_Shares"
                      ,"Research & Development":"Research_Development",
                      "Depreciation & Amortization":"Depreciation_Amortization"})

print(df.columns)
print(df.isnull().sum())

df.to_csv("us_income_annual_fixed.csv",index=False)