"""
data_provider.py

Abstraction layer for market data ingestion.
All other modules call get_price_data() — never yfinance directly.
"""

import yfinance as yf
import pandas as pd

#: str specifies the input types, the -> pd.DataFrame specifies the output type.
def get_price_data(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Fetch daily OHLCV price data for a single ticker.

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
    """Fetch adjusted close prices for multiple tickers.

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


if __name__ == '__main__':
    # Quick test — run with: python data_provider.py
    df = get_price_data('NVDA', '2024-01-01', '2026-01-01')
    print(df.head())
    print(df.shape)

    df_2 = get_close_prices('NVDA', '2024-01-01', '2026-01-01')
    print(df_2.head())
    print(df_2.shape)
