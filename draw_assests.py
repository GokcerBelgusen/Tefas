import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# SQLite database setup
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"

# Connect to the SQLite database
conn = sqlite3.connect(db_path)

# Define the query to retrieve asset data from the Fund_Assets table
fund_code = "IVY"  # Replace this with the actual fund code you want to query

query_assets = f"""
SELECT Date, Percent, Name
FROM Fund_Assets
WHERE Fund_Code = '{fund_code}'
ORDER BY Date ASC;
"""

# Load asset data from the SQLite database into a pandas DataFrame
df_assets = pd.read_sql_query(query_assets, conn)

# Convert the 'Date' column to datetime
df_assets['Date'] = pd.to_datetime(df_assets['Date'])

# Group the data by Date and Name, and take the mean of Percent to handle duplicates
df_assets_grouped = df_assets.groupby(['Date', 'Name'], as_index=False)['Percent'].mean()

# Pivot the DataFrame to get assets as columns for easier plotting
df_assets_pivot = df_assets_grouped.pivot(index='Date', columns='Name', values='Percent')

# Plot the changes in asset percentages over time on the same axis
plt.figure(figsize=(12, 6))

# Iterate through all assets (columns) and plot them on the same axis
for column in df_assets_pivot.columns:
    plt.plot(df_assets_pivot.index, df_assets_pivot[column], marker='.', label=column)

# Add labels and title
plt.title(f'Asset Composition Over Time for Fund {fund_code}')
plt.xlabel('Date')
plt.ylabel('Asset Percentage (%)')
plt.legend(title="Asset Name")
plt.grid(True)

# Display the plot with rotated date labels
plt.xticks(rotation=45)
plt.tight_layout()

# Show the plot
plt.show()

# Close the database connection
conn.close()
