"""
main.py

Entry point for the AI-Market Correlation Engine.
Runs the full pipeline: load → clean → analyse → visualize.

To add a new ticker, change TICKERS — nothing else needs to change.
"""

import pandas as pd
from pathlib import Path
from src.data_cleaning import clean_data
from src.analysis      import compute_returns, compute_rolling_vol
from src.visualization import plot_performance

# --- Configuration — single source of truth ---
# This is the ONLY place tickers, dates, or paths are defined.
# Adding 'TSLA' here is the only change needed to include a new stock.
TICKERS    = ['NVDA', 'MU', 'MSFT']
START_DATE = '2024-01-01'
END_DATE   = '2026-01-01'

# --- Step 1: Load and clean ---
close_prices = {}

for ticker in TICKERS:
    raw = pd.read_csv(Path('data') / f'{ticker}.csv', index_col=0)
    df = raw.iloc[2:].astype(float)
    df.index = pd.to_datetime(df.index) # From string to DateTime
    close_prices[ticker] = clean_data(df)['Close']

close = pd.DataFrame(close_prices)

# --- Step 2: Compute metrics ---
returns = compute_returns(close)
cum_returns = (1+returns).cumprod()-1
rolling_vol = compute_rolling_vol(close)

# --- Step 3: Visualize ---
plot_performance(cum_returns, rolling_vol)
print('Pipeline complete.')
