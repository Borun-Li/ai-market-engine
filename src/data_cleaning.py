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

__all__ = ['clean_data', 'normalize_minmax', 'normalize_zscore', 'flag_event_days']

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