"""
visualization.py

Functions for generating publication-quality performance charts.
All functions accept pre-computed DataFrames — no data loading here.
"""

import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from pathlib import Path

__all__ = ['plot_performance']

COLORS = {'NVDA': '#76B900', 'MU': '#0057B8', 'MSFT': '#0078D4'}


def plot_performance(
    cum_returns: pd.DataFrame,
    rolling_vol: pd.DataFrame,
    output_path: str = 'charts/performance_overview.png'
) -> None:
    """Generate and save a two-panel performance chart.

    Panel 1: Cumulative returns for all tickers.
    Panel 2: 20-day rolling volatility for all tickers.

    Args:
        cum_returns:  DataFrame of cumulative returns (one column per ticker).
        rolling_vol:  DataFrame of rolling volatility (one column per ticker).
        output_path:  File path to save the PNG. Default: charts/performance_overview.png.
    """
    # Create the charts/ folder if it doesn't exist yet
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    tickers = cum_returns.columns.tolist()
    colors  = {t: COLORS.get(t, '#333333') for t in tickers}

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(14, 9), sharex=True,
        gridspec_kw={'height_ratios': [3, 1]}
    )

    # --- Panel 1: Cumulative Returns ---
    for ticker in tickers:
        ax1.plot(cum_returns.index, cum_returns[ticker],
                label=ticker, color=colors[ticker], linewidth=1.8)
    ax1.axhline(y=0, color='grey', linewidth=0.8, linestyle='--')
    ax1.set_ylabel('Cumulative Return', fontsize=11)
    ax1.legend(frameon=False)

    # --- Panel 2: Rolling Volatility ---
    for ticker in tickers:
        ax2.plot(rolling_vol.index, rolling_vol[ticker],
                color=colors[ticker], linewidth=1.2)
    ax2.set_ylabel('20-Day Volatility', fontsize=11)
    ax2.set_xlabel('Date', fontsize=11)

    # Remove top and right borders from both panels
    for spine in ['top', 'right']:
        ax1.spines[spine].set_visible(False)
        ax2.spines[spine].set_visible(False)

    fig.suptitle('Performance & Volatility', fontsize=14, fontweight='bold')
    plt.tight_layout() # Must be called BEFORE savefig
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"Chart saved to {output_path}")
