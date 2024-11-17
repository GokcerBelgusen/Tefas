import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# SQLite database setup
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"

# Connect to the SQLite database
conn = sqlite3.connect(db_path)

# Define the query to retrieve price data from the Fund_History table
fund_code = "TGR"  # Replace this with the actual fund code you want to query

query = f"""
SELECT Date, Price
FROM Fund_History
WHERE Fund_Code = '{fund_code}'
ORDER BY Date ASC;
"""

# Load data from the SQLite database into a pandas DataFrame
df = pd.read_sql_query(query, conn)

# Convert the 'Date' column to datetime
df['Date'] = pd.to_datetime(df['Date'])

# Plot the price
plt.figure(figsize=(10, 6))
plt.plot(df['Date'], df['Price'], marker='.', linestyle='-', color='b')

# Add labels and title
plt.title(f'Price Over Time for Fußnd {fund_code}')
plt.xlabel('Date')
plt.ylabel('Price')
plt.grid(True)

# Display the plot
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Close the database connection
conn.close()
