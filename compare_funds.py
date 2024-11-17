import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# SQLite database setup
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"

# Connect to the SQLite database
conn = sqlite3.connect(db_path)

# Define the query to retrieve price data for Fund 1
fund_code_1 = "TGR"  # Replace with the first fund code
query_1 = f"""
SELECT Date, Price
FROM Fund_History
WHERE Fund_Code = '{fund_code_1}'
ORDER BY Date ASC;
"""

# Load data for Fund 1 from the SQLite database
df_fund_1 = pd.read_sql_query(query_1, conn)

# Define the query to retrieve price data for Fund 2
fund_code_2 = "GZY"  # Replace with the second fund code
query_2 = f"""
SELECT Date, Price
FROM Fund_History
WHERE Fund_Code = '{fund_code_2}'
ORDER BY Date ASC;
"""

# Load data for Fund 2 from the SQLite database
df_fund_2 = pd.read_sql_query(query_2, conn)

# Convert the 'Date' column to datetime for both dataframes
df_fund_1['Date'] = pd.to_datetime(df_fund_1['Date'])
df_fund_2['Date'] = pd.to_datetime(df_fund_2['Date'])

# Plot both funds on the same graph
plt.figure(figsize=(10, 6))

# Plot Fund 1's price data
plt.plot(df_fund_1['Date'], df_fund_1['Price'], marker='o', linestyle='-', label=f'Fund {fund_code_1}', color='b')

# Plot Fund 2's price data
plt.plot(df_fund_2['Date'], df_fund_2['Price'], marker='x', linestyle='-', label=f'Fund {fund_code_2}', color='r')

# Add labels and title
plt.title(f'Price Comparison Over Time for Funds {fund_code_1} and {fund_code_2}')
plt.xlabel('Date')
plt.ylabel('Price')
plt.legend()
plt.grid(True)

# Rotate date labels for better visibility
plt.xticks(rotation=45)

# Display the plot
plt.tight_layout()
plt.show()

# Close the database connection
conn.close()
