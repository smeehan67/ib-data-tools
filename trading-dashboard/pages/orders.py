"""
Order Management page.
Shows working orders across IB and Schwab.
"""

import streamlit as st
import pandas as pd

import data_loader
import config


st.title("Order Management")

with st.sidebar:
    st.subheader("Data Freshness")
    freshness = {
        "IB Orders": config.IB_OPEN_ORDERS,
        "Schwab Orders": config.SCHWAB_OPEN_ORDERS,
    }
    for label, path in freshness.items():
        age = data_loader.file_age_str(path)
        st.text(f"{label}: {age}")

    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()


# --- IB Open Orders ---
st.header("IB Working Orders")

ib_orders = data_loader.load_ib_open_orders()
if ib_orders is not None and not ib_orders.empty:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Orders", len(ib_orders))

    buys = ib_orders[ib_orders["ACTION"] == "BUY"]
    sells = ib_orders[ib_orders["ACTION"] == "SELL"]
    col2.metric("Buys", len(buys))
    col3.metric("Sells", len(sells))

    # Color-coded display
    display_cols = ["SYMBOL", "ACTION", "QTY", "ORDER_TYPE", "PRICE", "DESCRIPTION", "STATUS", "ACCOUNT"]
    available = [c for c in display_cols if c in ib_orders.columns]
    st.dataframe(
        ib_orders[available],
        use_container_width=True,
        hide_index=True,
        column_config={
            "PRICE": st.column_config.NumberColumn(format="%.2f"),
        },
    )

    # Summary by type
    with st.expander("Orders by type"):
        type_summary = ib_orders.groupby("ORDER_TYPE").agg(
            count=("ORDER_ID", "count"),
            total_qty=("QTY", "sum"),
        ).reset_index()
        st.dataframe(type_summary, use_container_width=True, hide_index=True)
else:
    st.info("No IB open orders found.")


st.divider()


# --- Schwab Open Orders ---
st.header("Schwab Working Orders")

schwab_orders = data_loader.load_schwab_open_orders()
if schwab_orders is not None and not schwab_orders.empty:
    col1, col2 = st.columns(2)
    col1.metric("Total Orders", len(schwab_orders))

    display_cols = ["DESCRIPTION", "ACTION", "QTY", "ORDER_TYPE", "STRATEGY", "PRICE", "STATUS", "ACCOUNT"]
    available = [c for c in display_cols if c in schwab_orders.columns]

    st.dataframe(
        schwab_orders[available],
        use_container_width=True,
        hide_index=True,
    )

    if "ENTERED" in schwab_orders.columns:
        with st.expander("Full order details"):
            st.dataframe(schwab_orders, use_container_width=True, hide_index=True)
else:
    st.info("No Schwab open orders found.")


st.divider()


# --- Combined Order Count ---
st.header("Summary")

ib_count = len(ib_orders) if ib_orders is not None else 0
schwab_count = len(schwab_orders) if schwab_orders is not None else 0

col1, col2, col3 = st.columns(3)
col1.metric("IB Orders", ib_count)
col2.metric("Schwab Orders", schwab_count)
col3.metric("Total Working", ib_count + schwab_count)
