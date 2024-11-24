import yfinance as yf
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta

# Define portfolio and weights
portfolio = {
    "ONEQ": 0.08,
    "SCHB": 0.12,
    "SPY": 0.15,
    "VUG": 0.10,
    "VTI": 0.15,
    "SCHG": 0.08,
    "SPMO": 0.05,
    "VYM": 0.10,
    "QQQM": 0.08,
    "VONG": 0.05,
    "SPLV": 0.03,
    "MAGS": 0.01,
}
investment = 100000



# Fetch historical data for the last two years
end_date = datetime.now()
start_date = end_date - timedelta(days=2*365)
data = yf.download(list(portfolio.keys()), start=start_date, end=end_date)["Adj Close"]

# Normalize data to start from the same base
normalized_data = data / data.iloc[0]

# Calculate portfolio value over time
weights = pd.Series(portfolio) * investment
portfolio_value = (normalized_data * weights).sum(axis=1)

# Plot the portfolio growth
plt.figure(figsize=(12, 6))
plt.plot(portfolio_value, label="Portfolio")
plt.title("Portfolio Growth Over Last 2 Years")
plt.xlabel("Date")
plt.ylabel("Portfolio Value (USD)")
plt.legend()
plt.grid()
plt.show()
