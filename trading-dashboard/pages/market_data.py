"""
Market Data page.
Shows implied volatility, historical price charts, and IV term structure.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

import data_loader
import config


st.title("Market Data")

with st.sidebar:
    st.subheader("Data Freshness")
    freshness = {
        "IV Data": config.IMPLIED_VOL,
        "Futures History": config.HISTORICAL_DATA,
        "ETF History": config.ETF_DATA,
    }
    for label, path in freshness.items():
        age = data_loader.file_age_str(path)
        st.text(f"{label}: {age}")

    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()


# --- Implied Volatility ---
st.header("Implied Volatility")

iv_data = data_loader.load_implied_volatility()
if iv_data is not None and not iv_data.empty:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Front Month IV")
        front = iv_data[["Symbol", "IV_Front", "DTE_Front"]].dropna(subset=["IV_Front"]).copy()
        front["IV_Front"] = front["IV_Front"].apply(lambda x: f"{x:.1%}")
        st.dataframe(front, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Second Month IV")
        second = iv_data[["Symbol", "IV_Second", "DTE_Second"]].dropna(subset=["IV_Second"]).copy()
        second["IV_Second"] = second["IV_Second"].apply(lambda x: f"{x:.1%}")
        st.dataframe(second, use_container_width=True, hide_index=True)

    # IV comparison chart
    st.subheader("IV Term Structure")
    chart_data = iv_data.dropna(subset=["IV_Front", "IV_Second"]).copy()
    if not chart_data.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="Front",
            x=chart_data["Symbol"],
            y=chart_data["IV_Front"],
            marker_color="#58a6ff",
        ))
        fig.add_trace(go.Bar(
            name="Second",
            x=chart_data["Symbol"],
            y=chart_data["IV_Second"],
            marker_color="#3fb950",
        ))
        fig.update_layout(
            barmode="group",
            yaxis_tickformat=".0%",
            height=400,
            template="plotly_dark",
            margin=dict(t=20, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Contango / backwardation indicator
    st.subheader("Term Structure Spread")
    spread_data = iv_data.dropna(subset=["IV_Front", "IV_Second"]).copy()
    if not spread_data.empty:
        spread_data["Spread"] = spread_data["IV_Second"] - spread_data["IV_Front"]
        spread_data["Structure"] = spread_data["Spread"].apply(
            lambda x: "Contango" if x > 0 else "Backwardation"
        )
        spread_display = spread_data[["Symbol", "IV_Front", "IV_Second", "Spread", "Structure"]].copy()
        spread_display["IV_Front"] = spread_display["IV_Front"].apply(lambda x: f"{x:.1%}")
        spread_display["IV_Second"] = spread_display["IV_Second"].apply(lambda x: f"{x:.1%}")
        spread_display["Spread"] = spread_display["Spread"].apply(lambda x: f"{x:+.1%}")
        st.dataframe(spread_display, use_container_width=True, hide_index=True)
else:
    st.warning("IV data not found. Run implied_volatility_grabber.py first.")


st.divider()


# --- Historical Price Charts ---
st.header("Historical Prices")

symbols = data_loader.get_historical_symbols()
all_futures = symbols.get("futures", [])
all_etfs = symbols.get("etf", [])

if all_futures or all_etfs:
    source_tab, = st.tabs(["Charts"])
    with source_tab:
        col1, col2 = st.columns(2)
        with col1:
            source = st.radio("Source", ["Futures", "ETFs"], horizontal=True)
        symbol_list = all_futures if source == "Futures" else all_etfs
        source_key = "futures" if source == "Futures" else "etf"

        if symbol_list:
            with col2:
                selected = st.selectbox("Symbol", symbol_list)

            if selected:
                df = data_loader.load_historical_data(selected, source_key)
                if df is not None and not df.empty:
                    # Try to identify the date and close columns
                    date_col = None
                    close_col = None
                    for c in df.columns:
                        cl = c.lower()
                        if "date" in cl:
                            date_col = c
                        if cl in ("close", "adj close", "adjusted_last"):
                            close_col = c

                    if date_col and close_col:
                        # Dates may be integers in YYYYMMDD format
                        df[date_col] = pd.to_datetime(df[date_col].astype(str), format="%Y%m%d", errors="coerce")
                        df = df.dropna(subset=[date_col, close_col])

                        # Lookback selector
                        lookback = st.select_slider(
                            "Lookback",
                            options=["1M", "3M", "6M", "1Y", "2Y", "All"],
                            value="1Y",
                        )
                        lookback_map = {"1M": 21, "3M": 63, "6M": 126, "1Y": 252, "2Y": 504, "All": len(df)}
                        n_bars = lookback_map[lookback]
                        plot_df = df.tail(n_bars)

                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=plot_df[date_col],
                            y=plot_df[close_col],
                            mode="lines",
                            name=selected,
                            line=dict(color="#58a6ff", width=1.5),
                        ))
                        fig.update_layout(
                            height=450,
                            template="plotly_dark",
                            margin=dict(t=20, b=40),
                            xaxis_title="",
                            yaxis_title="Price",
                        )
                        st.plotly_chart(fig, use_container_width=True)

                        # Quick stats
                        latest = plot_df[close_col].iloc[-1]
                        prev = plot_df[close_col].iloc[0]
                        change = latest - prev
                        pct = (change / prev) * 100 if prev != 0 else 0

                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Last", f"{latest:,.2f}")
                        c2.metric("Change", f"{change:+,.2f}")
                        c3.metric("% Change", f"{pct:+.1f}%")
                        c4.metric("Bars", len(plot_df))
                    else:
                        st.warning(f"Could not identify date/close columns in {selected}. Columns: {list(df.columns)}")
                        st.dataframe(df.head(), use_container_width=True)
                else:
                    st.info(f"No data for {selected}.")
        else:
            st.info("No symbols available.")
else:
    st.warning("No historical data files found.")
