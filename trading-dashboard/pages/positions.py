"""
Positions & Risk page.
Shows portfolio delta, Schwab positions, last prices, and cash balances.
"""

import streamlit as st
import pandas as pd

import data_loader
import config


st.title("Positions & Risk")

# Sidebar: data freshness
with st.sidebar:
    st.subheader("Data Freshness")
    freshness = {
        "Greeks": config.GREEKS_OUTPUT,
        "Positions": config.SCHWAB_POSITIONS,
        "Last Prices": config.LAST_PRICES,
        "Cash": config.SCHWAB_CASH,
    }
    for label, path in freshness.items():
        age = data_loader.file_age_str(path)
        st.text(f"{label}: {age}")

    # Token health
    token = data_loader.load_token_status()
    if token:
        if token["healthy"]:
            st.success(f"Schwab token: {token['remaining_days']}d remaining")
        elif token["remaining_days"] is not None:
            st.error(f"Schwab token: {token['remaining_days']}d remaining")

    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()


# --- Portfolio Delta ---
st.header("Portfolio Delta")

greeks = data_loader.load_greeks()
if greeks is not None and not greeks.empty:
    total_delta = greeks["DELTA"].sum()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Portfolio Delta", f"{total_delta:,.1f}")
    col2.metric("Positions Tracked", len(greeks))

    nonzero = greeks[greeks["DELTA"] != 0]
    col3.metric("Nonzero Delta", len(nonzero))

    # Delta breakdown
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Largest Positive Delta")
        positive = greeks[greeks["DELTA"] > 0].sort_values("DELTA", ascending=False).head(10)
        if not positive.empty:
            st.dataframe(positive, use_container_width=True, hide_index=True)
        else:
            st.info("No positive delta positions.")

    with col_right:
        st.subheader("Largest Negative Delta")
        negative = greeks[greeks["DELTA"] < 0].sort_values("DELTA").head(10)
        if not negative.empty:
            st.dataframe(negative, use_container_width=True, hide_index=True)
        else:
            st.info("No negative delta positions.")

    with st.expander("Full delta table"):
        st.dataframe(greeks.sort_values("DELTA", ascending=False), use_container_width=True, hide_index=True)
else:
    st.warning("Greeks output not found. Run aggregate_greeks.py first.")


st.divider()


# --- Schwab Positions ---
st.header("Schwab Positions")

positions = data_loader.load_schwab_positions()
if positions is not None and not positions.empty:
    accounts = positions["ACCOUNT"].unique()

    for acct in accounts:
        acct_df = positions[positions["ACCOUNT"] == acct][["SYMBOL", "POSITION"]].sort_values("SYMBOL")
        with st.expander(f"Account ...{str(acct)[-4:]}", expanded=len(accounts) == 1):
            col1, col2 = st.columns([1, 3])
            col1.metric("Positions", len(acct_df))
            st.dataframe(acct_df, use_container_width=True, hide_index=True)
else:
    st.warning("Schwab positions not found. Run schwab_positions.py first.")


st.divider()


# --- Cash Balances ---
st.header("Cash Balances")

cash = data_loader.load_schwab_cash()
if cash is not None and not cash.empty:
    cols = st.columns(len(cash))
    for i, row in cash.iterrows():
        acct = str(row["ACCOUNT"])
        balance = row["CASH & SWEEP VEHICLE"]
        cols[i].metric(f"...{acct[-4:]}", f"${balance:,.2f}")
else:
    st.warning("Cash data not found. Run schwab_cash.py first.")


st.divider()


# --- Last Prices ---
st.header("Last Prices")

prices = data_loader.load_last_prices()
if prices is not None and not prices.empty:
    # Display as a compact grid of metrics
    n_cols = 4
    rows_needed = (len(prices) + n_cols - 1) // n_cols
    for row_idx in range(rows_needed):
        cols = st.columns(n_cols)
        for col_idx in range(n_cols):
            i = row_idx * n_cols + col_idx
            if i < len(prices):
                symbol = prices.iloc[i]["Symbol"]
                last = prices.iloc[i]["Last"]
                cols[col_idx].metric(symbol, f"${last:,.2f}")
else:
    st.warning("Last prices not found. Run get_last_prices.py first.")
