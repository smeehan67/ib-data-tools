"""
Trading Operations Dashboard
Reads output files from IB and Schwab trading scripts across the Vibe project.
Run: streamlit run app.py
"""

import streamlit as st

st.set_page_config(
    page_title="Trading Operations",
    page_icon="📊",
    layout="wide",
)

pages = {
    "Positions & Risk": [
        st.Page("pages/positions.py", title="Positions & Risk", icon="📊"),
    ],
    "Orders": [
        st.Page("pages/orders.py", title="Order Management", icon="📋"),
    ],
    "Market Data": [
        st.Page("pages/market_data.py", title="Market Data", icon="📈"),
    ],
}

pg = st.navigation(pages)
pg.run()
