import yfinance as yf
import pandas as pd

TICKERS = ['NVDA', 'MU', 'MSFT']
START = '2024-01-01'
END   = '2026-01-01'

# --- Download all tickers at once ---
# auto_adjust=True gives split/dividend-adjusted prices (always use this)
raw = yf.download(TICKERS, start=START, end=END, auto_adjust=True)

print("=== raw.shape ===")
print(raw.shape)           # (rows, columns) — expect roughly (504, 15)

print("\n=== raw.columns ===")
print(raw.columns)         # MultiIndex: (Price_Type, Ticker)

# Extract just the Close prices for all 3 tickers
close = raw['Close']       # DataFrame: rows=dates, cols=NVDA/MU/MSFT

print("\n=== close.head() ===")
print(close.head())        # first 5 rows

print("\n=== close.tail() ===")
print(close.tail())        # last 5 rows

print("\n=== close.dtypes ===")
print(close.dtypes)        # should all be float64

print("\n=== close.shape ===")
print(close.shape)         # expect (~504, 3)bnu

print("\n=== close.index ===")
print(close.index)         # DatetimeIndex — dates with freq='B' (business days)


# --- Save each ticker to its own CSV ---
# Download individual DataFrames per ticker
for ticker in TICKERS:
    df = yf.download(ticker, start=START, end=END, auto_adjust=True)
    filepath = f'data/{ticker}.csv' # csv: comma-separated values
    df.to_csv(filepath)
    print(f"Saved {filepath} — shape: {df.shape}")

# --- Verify: reload one and check it matches ---
nvda_reloaded = pd.read_csv('data/NVDA.csv', index_col=0, parse_dates=True) # Parse the index by specifying the index column
print("\n=== Reloaded NVDA.csv ===")
print(nvda_reloaded.head())
print(nvda_reloaded.tail())
print(nvda_reloaded.dtypes)


# # --- Example ---
# # --- Exploring a single Ticker object ---
# nvda = yf.Ticker('NVDA')

# # .fast_info: lightweight, quick-loading key stats
# print("\n=== fast_info ===")
# print(nvda.fast_info)

# # Print specific fast_info fields
# print(f"Last Price:    {nvda.fast_info['lastPrice']}")
# print(f"Market Cap:    {nvda.fast_info['marketCap']:,}") # insert a comma every three digits
# print(f"Currency:      {nvda.fast_info['currency']}")
# print(f"52-Week High:  {nvda.fast_info['yearHigh']}")
# print(f"52-Week Low:   {nvda.fast_info['yearLow']}")

# # .info: full metadata dictionary (slower, more fields)
# info = nvda.info
# # print(info.keys())
# print("\n=== 10 fields from .info ===")
# fields = [
#     'longName', 'sector', 'industry', 'country',
#     'fullTimeEmployees', 'trailingPE', 'forwardPE',
#     'dividendYield', 'beta', 'recommendationKey'
# ]
# for field in fields:
#     print(f"{field}: {info.get(field, 'N/A')}")
