'''
Practices pandas methods on NVDA, MU, and MSFT
'''

import pandas as pd
import matplotlib.pyplot as plt

# --- Load your saved CSVs ---
nvda = pd.read_csv('data/NVDA.csv', index_col=0, parse_dates=True)
mu   = pd.read_csv('data/MU.csv',   index_col=0, parse_dates=True)
msft = pd.read_csv('data/MSFT.csv', index_col=0, parse_dates=True)

# Combine all Close prices into one DataFrame
close = pd.DataFrame({
    'NVDA': nvda['Close'],
    'MU':   mu['Close'],
    'MSFT': msft['Close']
}).iloc[2:].apply(pd.to_numeric, errors='coerce')
close.index = pd.to_datetime(close.index)

print("=== close.shape ===")
print(close.shape)

print("\n=== close.head() ===")
print(close.head())

# --- loc: slice by date label ---
print("\n=== loc: first half of 2024 ===")
h1_2024 = close.loc['2024-01-02':'2024-06-28'] # Inclusive
print(h1_2024.shape)
print(h1_2024.head())

# --- iloc: slice by row number ---
print("\n=== iloc: first 10 rows ===")
print(close.iloc[0:10]) # Exclusive

print("\n=== iloc: last row ===")
print(close.iloc[-1])

# --- pct_change: daily returns ---
# A positive pct indicates an increase in stock prices
returns = close.pct_change()
print("\n=== returns.head() ===")
print(returns.head())   # first row will be NaN

returns = returns.dropna()   # remove the NaN first row
print("\n=== returns after dropna ===")
print(returns.head()) # the original first row will be dropped

# --- resample: change frequency ---
weekly  = close.resample('W').last()
monthly = close.resample('ME').last()

print("\n=== daily shape ===",  close.shape)
print("=== weekly shape ===",  weekly.shape)
print("=== monthly shape ===", monthly.shape)

# --- rolling: 20-day moving average ---
ma20 = close.rolling(window=20).mean()
print("\n=== 20-day moving average (first 25 rows) ===")
print(ma20.head(25))    # first 19 rows are NaN


# --- Cumulative returns ---
# Formula: (1 + r1) * (1 + r2) * ... - 1
# This answers: "if I invested $1 on day 1, what is it worth now?"
cum_returns = (1 + returns).cumprod() - 1

print("\n=== Total return by end of period ===")
print(cum_returns.iloc[-1].sort_values(ascending=False))

# --- Plot cumulative returns ---
fig, ax = plt.subplots(figsize=(12, 6))

for ticker in ['NVDA', 'MU', 'MSFT']:
    # cum_returns.index: x-axis, cum_returns[ticker]: y-axis
    ax.plot(cum_returns.index, cum_returns[ticker], label=ticker)

ax.axhline(y=0, color='black', linewidth=0.8, linestyle='--')
ax.set_title('Cumulative Returns: NVDA / MU / MSFT')
ax.set_ylabel('Cumulative Return')
ax.set_xlabel('Date')
ax.legend()

# Order: customize → tight_layout → savefig → show
plt.tight_layout()
plt.savefig('data/returns_chart.png', dpi=150) #dpi: resolution (dots per inch)
plt.show()

print("\n=== Volatility (std of daily returns) ===")
print(returns.std().sort_values(ascending=False))

# --- Interpretation: Cumulative Returns & Volatility (2024–2026) ---
# Among the three semiconductors and tech stocks analyzed, NVDA demonstrates
# the highest cumulative return over the 2024–2026 period, significantly
# outperforming both MU and MSFT. This outperformance, however, comes with
# the highest daily volatility as confirmed by returns.std() — NVDA exhibits
# the widest daily price swings of the three tickers. This is consistent with
# NVDA's position as the primary beneficiary of the AI infrastructure boom,
# where surging demand for H100/H200 GPUs drove explosive price appreciation
# but also made the stock highly sensitive to earnings surprises, export
# restriction news, and shifts in AI spending sentiment. MU and MSFT, by
# contrast, show more moderate and stable return profiles — MSFT in particular
# reflects a large-cap stock where consistent Azure cloud revenue growth dampens
# volatility relative to pure-play semiconductor names like NVDA and MU.
# Key takeaway: in this period, higher volatility (NVDA) was rewarded with
# higher returns, illustrating the classic risk-return tradeoff in equity markets.

