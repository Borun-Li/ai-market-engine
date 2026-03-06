"""
analysis.py

Pure functions for computing financial time-series metrics.
Input: cleaned close price DataFrames.
Output: derived metric DataFrames. No side effects.
"""

import pandas as pd

__all__ = ['compute_returns', 'compute_rolling_vol']


def compute_returns(close: pd.DataFrame) -> pd.DataFrame:
    """Compute daily percentage returns for each ticker.

    Args:
        close: DataFrame with DatetimeIndex, one column per ticker.

    Returns:
        DataFrame of daily returns with the first row dropped (NaN from pct_change).
    """
    return close.pct_change().dropna()


def compute_rolling_vol(returns: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """Compute rolling volatility (std of returns) for each ticker.

    Args:
        returns: DataFrame of daily returns (output of compute_returns).
        window:  Rolling window in trading days. Default is 20 (~1 month).

    Returns:
        DataFrame of rolling std values. First (window-1) rows will be NaN.
    """
    return returns.rolling(window=window).std()
