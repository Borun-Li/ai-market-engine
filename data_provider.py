"""
data_provider.py

Abstraction layer for market data ingestion.
All other modules call get_price_data() — never yfinance directly.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
# datetime: Combines a date and a time into a single object
# timedelta: Represents a duration between two date or datetime instances

#: str specifies the input types, the -> pd.DataFrame specifies the output type.
def get_price_data(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch daily OHLCV price data for a single ticker.

    Parameters:
        ticker: Stock symbol, e.g. 'NVDA'
        start:  Start date string, e.g. '2023-01-01'
        end:    End date string, e.g. '2025-01-01'

    Returns:
        DataFrame with DatetimeIndex and columns: Open, High, Low, Close, Volume
    """
    df = yf.download(ticker, start=start, end=end, auto_adjust=True)

    if df.empty:
        raise ValueError(f"No data returned for ticker: {ticker}")

    return df

def get_close_prices(tickers: list, start: str, end: str) -> pd.DataFrame:
    """
    Fetch adjusted close prices for multiple tickers.

    Parameters:
        tickers: List of stock symbols, e.g. ['NVDA', 'MU', 'MSFT']
        start:   Start date string
        end:     End date string

    Returns:
        DataFrame with DatetimeIndex, one column per ticker
    """
    raw = yf.download(tickers, start=start, end=end, auto_adjust=True)
    close = raw['Close']
    return close


class DataValidationError(Exception):
    """
    Raised when downloaded price data fails quality checks.
    """
    pass


def validate_data(df: pd.DataFrame, ticker: str, max_staleness_days: int = 5) -> None:
    """
    Validate price data quality before analysis.

    Checks:
        1. Most recent date is within max_staleness_days calendar days of today
        2. No gaps larger than 4 calendar days in the index
        3. No column has more than 2% NaN values

    Args:
        df: Price DataFrame with DatetimeIndex
        ticker: Ticker symbol (used in error messages)
        max_staleness_days: Max allowed days since last data point (default 5)

    Raises:
        DataValidationError: if any check fails
    """
    # Check 1: data is recent enough
    most_recent = df.index[-1].date()
    today = datetime.today().date()
    days_since_update = (today - most_recent).days

    if days_since_update > max_staleness_days:
        raise DataValidationError(
            f"{ticker}: data is stale. Most recent date is "
            f"{most_recent} ({days_since_update} days ago)."
        )

    # Check 2: no large gaps in the index
    date_diffs = pd.Series(df.index).diff().dropna()
    max_gap = date_diffs.max().days

    if max_gap > 4:
        raise DataValidationError(
            f"{ticker}: gap of {max_gap} calendar days found in index. "
            f"Possible missing data."
        )

    # Check 3: no column has more than 2% NaN
    nan_pct = df.isnull().mean()
    bad_cols = nan_pct[nan_pct > 0.02]

    if not bad_cols.empty:
        raise DataValidationError(
            f"{ticker}: columns exceed 2% NaN threshold:\n{bad_cols}"
        )

    print(f"{ticker}: data validation passed.")


if __name__ == '__main__':
    # Quick test — run with: python data_provider.py
    df = get_price_data('NVDA', '2024-01-01', '2026-01-01')
    print(df.head())
    print(df.shape)

    df_2 = get_close_prices('NVDA', '2024-01-01', '2026-01-01')
    print(df_2.head())
    print(df_2.shape)

    df = get_price_data('NVDA', '2024-01-01', '2026-01-01')
    validate_data(df, 'NVDA', max_staleness_days=60)