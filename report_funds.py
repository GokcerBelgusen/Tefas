import sqlite3
import pandas as pd

# Database path
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"

# SQL Query
query = """
SELECT
    fd.Code AS Fund_Code,
    -- fd.Title,
    fd.Category,
    round(((MAX(fh.Price) - MIN(fh.Price)) / MIN(fh.Price)) * 100, 3) AS YTD,
    round(l.R_squared, 3) AS Linearity,
    round(s.Sharpe_Ratio, 2) AS Sharpe_Ratio,
    fd.Market_Share
FROM
    Fund_Details fd
    JOIN Linearity_Analysis l ON fd.Code = l.Fund_Code
    JOIN Sharpe_Ratio_Analysis s ON fd.Code = s.Fund_Code
    JOIN Fund_History fh ON fd.Code = fh.Fund_Code
WHERE
    l.R_squared > 0.0 -- Adjust threshold for high linearity
    AND s.Sharpe_Ratio > 0.0 -- Adjust threshold for high Sharpe Ratio
    AND fh.Date BETWEEN '2024-01-01' AND '2024-11-15' -- Replace with the desired date range
GROUP BY
    fd.Code,
    fd.Title,
    fd.Category,
    fd.Market_Share,
    l.R_squared,
    s.Sharpe_Ratio
ORDER BY
    l.R_squared DESC,
    s.Sharpe_Ratio DESC;
"""

def fetch_and_display_metrics():
    # Connect to the database
    conn = sqlite3.connect(db_path)
    
    try:
        # Execute the query and load into DataFrame
        df = pd.read_sql_query(query, conn)
        
        # Display in tabular format
        if not df.empty:
            print("Fund Metrics:")
            print(df.to_markdown(index=False))  # Display as markdown table
        else:
            print("No records found matching the criteria.")
    
    finally:
        # Close the database connection
        conn.close()

# Run the function
fetch_and_display_metrics()
