from __future__ import annotations

import shutil

import plotly.graph_objects as go
import streamlit as st

from app.config import load_config
from app.dashboard import queries
from app.db.models import init_db
from app.db.session import get_engine


st.set_page_config(page_title="Scalperkuy", layout="wide")


@st.cache_resource
def engine_resource():
    engine = get_engine()
    init_db(engine)
    return engine


config = load_config()
engine = engine_resource()

st.title("Scalperkuy")
st.caption("Paper mode only. No live trading, no leverage, no trading API key.")

page = st.sidebar.radio("Page", ["System", "Market", "Paper Trading", "Journal"])
st.sidebar.write(f"Mode: `{config.mode}`")
st.sidebar.write(f"Timezone: `{config.timezone}`")


if page == "System":
    st.subheader("System")
    health = queries.latest_service_health(engine)
    counts = queries.table_counts(engine)
    recent = queries.recent_health_events(engine)
    disk = shutil.disk_usage("/")

    col1, col2, col3 = st.columns(3)
    col1.metric("Disk used", f"{disk.used / disk.total * 100:.1f}%")
    col2.metric("Disk free", f"{disk.free / (1024 ** 3):.1f} GB")
    col3.metric("Tracked symbols", len(config.symbols))

    st.write("Service status")
    st.dataframe(health, use_container_width=True, hide_index=True)

    st.write("Database row counts")
    st.dataframe(counts, use_container_width=True, hide_index=True)

    st.write("Recent service events")
    st.dataframe(recent, use_container_width=True, hide_index=True)

elif page == "Market":
    st.subheader("Market")
    quotes = queries.latest_quotes(engine)
    candles = queries.latest_candles(engine)

    if quotes.empty:
        st.info("No quote data yet. Start the collector and wait for the first poll.")
    else:
        st.write("Latest quotes")
        st.dataframe(quotes, use_container_width=True, hide_index=True)

    if not candles.empty:
        st.write("Latest candles")
        st.dataframe(candles, use_container_width=True, hide_index=True)

    symbol = st.selectbox("Candle chart symbol", config.symbols)
    history = queries.candle_history(engine, symbol)
    if history.empty:
        st.info(f"No candle history yet for {symbol}.")
    else:
        fig = go.Figure(
            data=[
                go.Candlestick(
                    x=history["open_time"],
                    open=history["open"],
                    high=history["high"],
                    low=history["low"],
                    close=history["close"],
                )
            ]
        )
        fig.update_layout(height=520, xaxis_rangeslider_visible=False)
        st.plotly_chart(fig, use_container_width=True)

elif page == "Paper Trading":
    st.subheader("Paper Trading")
    st.info("Paper trader execution is intentionally not enabled in milestone 1. Market collection stays independent.")
    trades = queries.recent_trades(engine)
    st.write("Recent paper trades")
    st.dataframe(trades, use_container_width=True, hide_index=True)

else:
    st.subheader("Journal")
    st.info("Daily and weekly Gemini summaries are planned after the collector and paper trading loop are stable.")
