import sqlite3
import pandas as pd
import numpy as np

# Path to your SQLite database
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"

def fetch_fund_data(fund_code, db_path):
    """
    Fetch historical price data for a given fund from SQLite database.
    
    Parameters:
        fund_code (str): The fund code to fetch data for.
        db_path (str): Path to the SQLite database.

    Returns:
        pd.DataFrame: DataFrame with 'Date' and 'Price' columns.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    query = """
        SELECT Date, Price
        FROM Fund_History
        WHERE Fund_Code = ?
        ORDER BY Date ASC
    """
    # Execute the query and fetch the data
    df = pd.read_sql_query(query, conn, params=(fund_code,))
    conn.close()
    return df

def calculate_volatility_from_db(fund_code, db_path):
    """
    Calculate the volatility for a given fund using data from SQLite.
    
    Parameters:
        fund_code (str): The fund code to calculate volatility for.
        db_path (str): Path to the SQLite database.

    Returns:
        float: Annualized volatility as a percentage.
    """
    # Fetch the fund data
    fund_data = fetch_fund_data(fund_code, db_path)

    # Check if data is sufficient
    if len(fund_data) < 2:
        raise ValueError("Not enough data points to calculate volatility.")
    
    # Calculate daily returns
    fund_data['Daily_Return'] = fund_data['Price'].pct_change()
    
    # Calculate standard deviation of daily returns
    daily_volatility = fund_data['Daily_Return'].std()
    
    # Annualize the volatility (assuming 252 trading days)
    annualized_volatility = daily_volatility * np.sqrt(252)
    
    # Convert to percentage
    return annualized_volatility * 100

# Example Usage
fund_code = "AAK"  # Replace with your fund code
try:
    volatility = calculate_volatility_from_db(fund_code, db_path)
    print(f"Annualized Volatility for fund {fund_code}: {volatility:.2f}%")
except Exception as e:
    print(f"Error: {e}")
