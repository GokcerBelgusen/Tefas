import sqlite3
import pandas as pd
from scipy.stats import linregress

# Database path
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"

# Fund for analysis
fund_code = "RPD"

# SQL Query to fetch fund price data
query = f"""
SELECT Date, Price
FROM Fund_History
WHERE Fund_Code = '{fund_code}'
ORDER BY Date ASC;
"""

# Fetch data
conn = sqlite3.connect(db_path)
data = pd.read_sql_query(query, conn)
conn.close()

# Convert date to datetime and map to ordinal
data['Date'] = pd.to_datetime(data['Date'])
data['Time'] = data['Date'].map(pd.Timestamp.toordinal)

# Perform linear regression
slope, intercept, r_value, p_value, std_err = linregress(data['Time'], data['Price'])

# R-squared value
r_squared = r_value ** 2
print(f"Linearity (R-squared) for Fund {fund_code}: {r_squared:.4f}")
