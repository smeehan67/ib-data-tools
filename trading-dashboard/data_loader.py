"""
Data loading functions for all trading script output files.
Each function returns a DataFrame (or None if file not found).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

import config


def file_age_str(path: Path) -> str:
    """Return human-readable age of a file, or 'not found'."""
    if not path.exists():
        return "not found"
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    delta = datetime.now() - mtime
    if delta.total_seconds() < 60:
        return "just now"
    if delta.total_seconds() < 3600:
        mins = int(delta.total_seconds() / 60)
        return f"{mins}m ago"
    if delta.total_seconds() < 86400:
        hours = int(delta.total_seconds() / 3600)
        return f"{hours}h ago"
    days = int(delta.total_seconds() / 86400)
    return f"{days}d ago"


@st.cache_data(ttl=30)
def load_greeks() -> pd.DataFrame | None:
    """Load delta aggregation output: SYMBOL, DELTA."""
    if not config.GREEKS_OUTPUT.exists():
        return None
    df = pd.read_excel(config.GREEKS_OUTPUT)
    return df


@st.cache_data(ttl=30)
def load_ib_open_orders() -> pd.DataFrame | None:
    """Load IB open orders: ORDER_ID, ACCOUNT, STATUS, ORDER_TYPE, ACTION, QTY, PRICE, SYMBOL, SEC_TYPE, DESCRIPTION, CONID."""
    if not config.IB_OPEN_ORDERS.exists():
        return None
    df = pd.read_excel(config.IB_OPEN_ORDERS, sheet_name="Open Orders")
    return df


@st.cache_data(ttl=30)
def load_last_prices() -> pd.DataFrame | None:
    """Load last prices: Symbol, Last."""
    if not config.LAST_PRICES.exists():
        return None
    df = pd.read_excel(config.LAST_PRICES)
    return df


@st.cache_data(ttl=30)
def load_implied_volatility() -> pd.DataFrame | None:
    """Load IV data: Symbol, IV_Front, IV_Second, Expiry_Front, Expiry_Second, DTE_Front, DTE_Second."""
    if not config.IMPLIED_VOL.exists():
        return None
    df = pd.read_excel(config.IMPLIED_VOL)
    # Convert IV to percentage for display
    for col in ["IV_Front", "IV_Second"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=30)
def load_schwab_positions() -> pd.DataFrame | None:
    """Load Schwab positions across all account sheets."""
    if not config.SCHWAB_POSITIONS.exists():
        return None
    xls = pd.ExcelFile(config.SCHWAB_POSITIONS)
    frames = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        if "ACCOUNT" not in df.columns:
            df["ACCOUNT"] = sheet
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    # Filter out zero positions
    combined = combined[combined["POSITION"] != 0]
    return combined


@st.cache_data(ttl=30)
def load_schwab_cash() -> pd.DataFrame | None:
    """Load Schwab cash balances: ACCOUNT, CASH & SWEEP VEHICLE."""
    if not config.SCHWAB_CASH.exists():
        return None
    df = pd.read_excel(config.SCHWAB_CASH)
    return df


@st.cache_data(ttl=30)
def load_schwab_open_orders() -> pd.DataFrame | None:
    """Load Schwab open orders."""
    if not config.SCHWAB_OPEN_ORDERS.exists():
        return None
    df = pd.read_excel(config.SCHWAB_OPEN_ORDERS, sheet_name="Open Orders")
    return df


def load_token_status() -> dict | None:
    """Load Schwab token and return status info."""
    if not config.SCHWAB_TOKEN.exists():
        return None
    with open(config.SCHWAB_TOKEN) as f:
        data = json.load(f)
    token = data.get("token", data)
    creation = data.get("creation_timestamp")
    if creation:
        created_dt = datetime.fromtimestamp(creation, tz=timezone.utc)
        age_days = (datetime.now(tz=timezone.utc) - created_dt).days
        remaining = 7 - age_days
        return {
            "created": created_dt.strftime("%Y-%m-%d %H:%M UTC"),
            "age_days": age_days,
            "remaining_days": remaining,
            "healthy": remaining > 1,
        }
    return {"created": "unknown", "age_days": None, "remaining_days": None, "healthy": None}


@st.cache_data(ttl=300)
def load_historical_data(symbol: str, source: str = "futures") -> pd.DataFrame | None:
    """Load historical OHLC data for a symbol from the combined workbook."""
    path = config.HISTORICAL_DATA if source == "futures" else config.ETF_DATA
    if not path.exists():
        return None
    try:
        df = pd.read_excel(path, sheet_name=symbol)
        return df
    except (ValueError, KeyError):
        return None


@st.cache_data(ttl=300)
def get_historical_symbols() -> dict:
    """Get available symbols from both historical data workbooks."""
    result = {"futures": [], "etf": []}
    for source, path in [("futures", config.HISTORICAL_DATA), ("etf", config.ETF_DATA)]:
        if path.exists():
            xls = pd.ExcelFile(path)
            result[source] = xls.sheet_names
    return result
