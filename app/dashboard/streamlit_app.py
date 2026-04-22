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
    freshness = queries.market_data_freshness(engine, config.data.stale_data_seconds, config.symbols)
    quality = queries.data_quality_summary(engine)
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

    st.write("Market data freshness")
    st.dataframe(freshness, use_container_width=True, hide_index=True)

    st.write("Data quality, last 24h")
    st.dataframe(quality, use_container_width=True, hide_index=True)

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
    st.caption("Paper-only simulation. No live orders, no exchange private key.")
    performance = queries.paper_performance(engine)
    open_positions = queries.open_positions(engine)
    trades = queries.recent_trades(engine)
    signal_summary = queries.signal_summary(engine)
    recent_signals = queries.recent_signals(engine)
    equity_curve = queries.equity_curve(engine)

    if not performance.empty:
        row = performance.iloc[0]
        closed_trades = int(row["closed_trades"] or 0)
        wins = int(row["wins"] or 0)
        losses = int(row["losses"] or 0)
        gross_loss = float(row["gross_loss_idr"] or 0)
        gross_profit = float(row["gross_profit_idr"] or 0)
        win_rate = (wins / closed_trades * 100) if closed_trades else 0
        profit_factor = (gross_profit / gross_loss) if gross_loss else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Realized PnL", f"Rp{float(row['realized_pnl_idr']):,.0f}")
        col2.metric("Closed trades", closed_trades)
        col3.metric("Win rate", f"{win_rate:.1f}%")
        col4.metric("Profit factor", f"{profit_factor:.2f}")

        col5, col6, col7, col8 = st.columns(4)
        col5.metric("Open trades", int(row["open_trades"] or 0))
        col6.metric("Avg PnL", f"{float(row['avg_pnl_percent']):.3f}%")
        col7.metric("Fees", f"Rp{float(row['fees_idr']):,.0f}")
        col8.metric("Slippage", f"Rp{float(row['slippage_idr']):,.0f}")

    if not equity_curve.empty:
        fig = go.Figure(
            data=[
                go.Scatter(
                    x=equity_curve["exit_time"],
                    y=equity_curve["cumulative_pnl_idr"],
                    mode="lines+markers",
                    name="Cumulative PnL",
                )
            ]
        )
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)

    st.write("Open positions")
    st.dataframe(open_positions, use_container_width=True, hide_index=True)

    st.write("Recent paper trades")
    st.dataframe(trades, use_container_width=True, hide_index=True)

    st.write("Signal summary, last 24h")
    st.dataframe(signal_summary, use_container_width=True, hide_index=True)

    st.write("Recent signals")
    st.dataframe(recent_signals, use_container_width=True, hide_index=True)

else:
    st.subheader("Journal")
    st.caption("Deterministic research journal. Gemini is not used for these numbers.")
    latest_journal = queries.latest_journal_entry(engine)
    recent_journal = queries.recent_journal_entries(engine)

    if latest_journal.empty:
        st.info("No journal entry yet. Reporter will create one after its next interval.")
    else:
        row = latest_journal.iloc[0]
        st.write(f"Latest: {row['title']}")
        st.caption(f"Updated at {row['updated_at']}")
        st.text(row["summary"])

    st.write("Recent journal entries")
    st.dataframe(recent_journal, use_container_width=True, hide_index=True)
