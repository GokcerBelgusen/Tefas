import sqlite3
import pandas as pd
import numpy as np

# Database path
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"

def calculate_sharpe_ratio(fund_code, start_date, end_date, risk_free_rate=0.03):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    
    # Fetch fund history
    query = f"""
        SELECT Date, Price
        FROM Fund_History
        WHERE Fund_Code = '{fund_code}' AND Date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY Date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Calculate daily returns
    df['Return'] = df['Price'].pct_change()
    
    # Drop the first row with NaN return
    df = df.dropna(subset=['Return'])
    
    # Calculate metrics
    avg_return = df['Return'].mean()  # Average daily return
    volatility = df['Return'].std()  # Standard deviation of daily returns
    
    # Annualize the metrics
    trading_days = 252  # Typical number of trading days in a year
    annualized_return = avg_return * trading_days
    annualized_volatility = volatility * np.sqrt(trading_days)
    
    # Calculate Sharpe Ratio
    sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility
    
    # Print results
    print(f"Fund: {fund_code}")
    print(f"Average Annual Return: {annualized_return:.2%}")
    print(f"Annualized Volatility: {annualized_volatility:.2%}")
    print(f"Sharpe Ratio: {sharpe_ratio:.2f}")
    
    return sharpe_ratio

# Example usage
sharpe_ratio = calculate_sharpe_ratio("AAK", "2024-01-01", "2024-11-15")
