"""
File paths for all trading script outputs.
All paths are relative to the Vibe project root.
"""

from pathlib import Path

VIBE_ROOT = Path(__file__).resolve().parent.parent

# IB Delta aggregation
GREEKS_OUTPUT = VIBE_ROOT / "IB Delta aggregation" / "greeks_output.xlsx"

# IB open orders
IB_OPEN_ORDERS = VIBE_ROOT / "IB limit order adjustment" / "ib_open_orders.xlsx"

# Last prices
LAST_PRICES = VIBE_ROOT / "last prices" / "last_outputs.xlsx"

# Implied volatility
IMPLIED_VOL = VIBE_ROOT / "Volatility Data" / "implied_volatility.xlsx"

# Schwab positions
SCHWAB_POSITIONS = VIBE_ROOT / "Schwab API" / "schwab_positions.xlsx"

# Schwab cash
SCHWAB_CASH = VIBE_ROOT / "Schwab API" / "cash_sweep.xlsx"

# Schwab open orders
SCHWAB_OPEN_ORDERS = VIBE_ROOT / "Schwab API" / "open_orders.xlsx"

# Schwab token
SCHWAB_TOKEN = VIBE_ROOT / "Schwab API" / "schwab_token.json"

# Historical data
HISTORICAL_DATA = VIBE_ROOT / "historical data" / "futures_combined.xlsx"
ETF_DATA = VIBE_ROOT / "historical data" / "ETF_data_combined.xlsx"
