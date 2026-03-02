"""
data_cleaning.py

Functions for cleaning, validating, and normalizing price DataFrames.
All functions are pure — they return new DataFrames, never modify in place.
"""

# --- Data Audit Results (NVDA, MU, MSFT | 2024-01-01 to 2026-01-01) ---
# Rows:           504 entries per ticker (503 real trading days + 1 junk row)
# Dtypes:         All columns are str instead of float64 — caused by the
#                 MultiIndex header row ("Ticker") saved into the CSV by
#                 yf.download(), which prevents pandas from inferring numeric types
# NaN count:      1 per column (5 total) — the junk "Ticker" row has no data value
# Zero prices:    None detected in any ticker
# Negative prices: None detected in any ticker
# Fix applied:    .iloc[2:].astype(float) skips the junk row and restores
#                 correct float64 dtype before any analysis or cleaning

import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a raw OHLCV price DataFrame.

    Operations performed:
        1. Forward-fill missing prices (handles weekends/holidays)
        2. Backward-fill any remaining NaNs at the start of the series
        3. Drop any rows that are still NaN after both fills
        4. Assert no zero or negative prices exist

    Args:
        df: Raw OHLCV DataFrame with DatetimeIndex

    Returns:
        Cleaned DataFrame with same columns, potentially fewer rows
    """
    if df.empty:
        raise ValueError("Input DataFrame is empty.")

    # Step 1: Protect the Original Data
    df_clean = df.copy()

    # Step 2: Fill and Drop
    df_clean = df_clean.ffill().bfill().dropna()

    # Step 3: Defensive Validation
    if 'Close' not in df_clean.columns:
        # KeyError: occurs when try to access a dict key that does not exist
        raise KeyError("Input must contain a 'Close' column.")

    # Step 4: check for impossible prices
    if (df_clean['Close'] <= 0).any():
        bad_dates = df_clean[df_clean['Close'] <= 0].index.tolist() # Slightly faster than list(df.index)
        raise ValueError(f"Zero or negative Close prices found on: {bad_dates}")

    rows_removed = len(df) - len(df_clean)
    print(f"clean_data: removed {rows_removed} rows, {len(df_clean)} rows remain.")

    return df_clean


def normalize_minmax(series: pd.Series) -> pd.Series:
    """Scale a Series to the [0, 1] range.

    Formula: (x - min) / (max - min)

    Args:
        series: Numeric pandas Series

    Returns:
        Series with values scaled to [0, 1]

    Raises:
        ValueError: if the series is empty
                    or constant (max == min)
    """
    if series.empty:
        raise ValueError("Cannot normalize an empty Series.")
    
    min_val = series.min()
    max_val = series.max()

    if max_val == min_val:
        raise ValueError(f"Cannot normalize constant series: {series.name}")

    # Broadcasting in Pandas
    return (series - min_val) / (max_val - min_val)


def normalize_zscore(series: pd.Series) -> pd.Series:
    """Standardize a Series to mean=0, std=1 using Z-score normalization.

    Formula: (x - mean) / std

    Args:
        series: Numeric pandas Series

    Returns:
        A standardized Series. If the input has no variation (std=0)
        or contains only one element, returns a Series of 0.0.
    """
    mean = series.mean()
    std = series.std(ddof=1) # ddof: delta degrees of freedom

    if std > 0:
        return (series - mean) / std
    else: # If std is 0 or NaN (single element), return 0s of the same shape
        return series - series.mean()


def flag_event_days(returns: pd.Series, threshold: float = 3.0) -> pd.Series:
    """Flag days where the absolute return exceeds threshold standard deviations.

    Uses the 3-sigma rule: days where abs(return) > 3 * std are outliers.
    These are NOT removed — they are labeled so analysis can run
    both with and without them.

    Args:
        returns:   Series of daily returns (output of pct_change())
        threshold: Number of standard deviations to use as cutoff (default 3.0)

    Returns:
        Boolean Series — True means that day is an outlier/event day
    """
    std = returns.std()
    mu = returns.mean()
    return (returns - mu).abs() > threshold * std


if __name__ == '__main__':
    import pandas as pd

    # for ticker in ['NVDA', 'MU', 'MSFT']:
    #     print(f"\n{'='*3} {ticker} {'='*3}")

    #     raw = pd.read_csv(f'data/{ticker}.csv', index_col=0)
    #     df  = raw.iloc[2:].astype(float)
    #     df.index = pd.to_datetime(df.index)   # parse dates after junk row is gone
    #     cleaned = clean_data(df)

    #     print(f"NaN count after cleaning: {cleaned.isnull().sum().sum()}")

    #     close = cleaned['Close']

    #     scaled = normalize_minmax(close)
    #     print(f"Min-Max — min: {scaled.min():.4f}  max: {scaled.max():.4f}")

    #     standardized = normalize_zscore(close)
    #     print(f"Z-Score — mean: {standardized.mean():.6f}  std: {standardized.std():.6f}")

    #     returns = close.pct_change().dropna()
    #     event_days = flag_event_days(returns)
    #     print(f"Event days flagged: {event_days.sum()}")
    #     print(returns[event_days])


    # --- Founder Challenge: NVDA-MU Correlation (with vs. without event days) ---
    print(f"\n{'='*5} FOUNDER CHALLENGE: NVDA-MU Correlation {'='*5}")

    # Step 1: Load and clean both tickers
    nvda_raw = pd.read_csv('data/NVDA.csv', index_col=0)
    nvda_df  = nvda_raw.iloc[2:].astype(float)
    nvda_df.index = pd.to_datetime(nvda_df.index)
    nvda_clean = clean_data(nvda_df)

    mu_raw = pd.read_csv('data/MU.csv', index_col=0)
    mu_df  = mu_raw.iloc[2:].astype(float)
    mu_df.index = pd.to_datetime(mu_df.index)
    mu_clean = clean_data(mu_df)

    # Step 2: Calculate daily returns for both
    # pct_change(): percentage change
    nvda_returns = nvda_clean['Close'].pct_change().dropna()
    mu_returns   = mu_clean['Close'].pct_change().dropna()

    # Step 3: Align the two series on the same dates
    # .align() returns two Series trimmed to the same DatetimeIndex
    nvda_returns, mu_returns = nvda_returns.align(mu_returns, join='inner')

    # Step 4: Correlation WITH all days
    corr_all = nvda_returns.corr(mu_returns)
    print(f"\nCorrelation (all days): {corr_all:.4f}")

    # Step 5: Flag event days for EITHER ticker
    nvda_events = flag_event_days(nvda_returns)
    mu_events   = flag_event_days(mu_returns)
    # | --> the element-wise OR operator for pandas boolean Series
    event_mask  = nvda_events | mu_events   # True if EITHER had a 3-sigma day

    print(f"Event days removed: {event_mask.sum()}")

    # Step 6: Correlation WITHOUT event days
    nvda_filtered = nvda_returns[~event_mask]   # ~ = NOT operator (flip True/False)
    mu_filtered   = mu_returns[~event_mask]

    corr_filtered = nvda_filtered.corr(mu_filtered)
    print(f"Correlation (without event days): {corr_filtered:.4f}")

    # Step 7: Compare
    diff = corr_filtered - corr_all
    print(f"Difference: {diff:+.4f}")

    # --- Interpretation ---
    # Correlation (all days):           0.5856
    # Correlation (without event days): 0.5462
    # Difference:                      -0.0394
    #
    # Removing the 12 extreme event days caused correlation to drop by ~0.04,
    # meaning event days were slightly inflating the measured relationship.
    # During 3-sigma moves — the DeepSeek crash (-16.9% NVDA, Jan 2025),
    # the tariff-pause surge (+18.7% NVDA, Apr 2025), and similar shocks —
    # both semiconductors tend to move together because panic selling and
    # macro relief rallies hit the entire sector indiscriminately, not because
    # of any structural link between their businesses.
    #
    # On normal trading days the correlation is still moderate (0.55), reflecting
    # a genuine but loose connection: both are cyclical semiconductor stocks
    # sensitive to AI capex trends and broad chip demand cycles. However, NVDA
    # is primarily driven by AI GPU demand (H100/H200 data center orders) while
    # MU is driven by DRAM/NAND memory pricing — different end markets that
    # diverge when sector-wide panic is absent.
    #
    # Key takeaway: the "true" structural correlation between NVDA and MU is
    # closer to 0.55 than 0.59. The gap is small but meaningful — it confirms
    # that a portion of their co-movement is event-driven noise, not a stable
    # day-to-day relationship that a trading strategy could reliably exploit.


