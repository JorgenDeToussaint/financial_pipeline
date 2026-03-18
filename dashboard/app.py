import streamlit as st
import polars as pl
import boto3
import io
import os
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Financial Lakehouse",
    page_icon="🌊",
    layout="wide"
)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

VIEWS = {
    "🌍 Macro Overview": "macro_overview",
    "🛡️ Defense Tracker": "defense_tracker",
    "⛏️ Commodities": "commodities",
    "💾 Semiconductors": "semiconductors",
    "🪙 Crypto & DeFi": "crypto_defi",
    "🔭 Full Universe": "full_universe",
}

@st.cache_data(ttl=300)
def load_view(view_id: str) -> pl.DataFrame:
    s3 = boto3.resource(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )
    obj = s3.Object("gold", f"views/{view_id}/data_daily.parquet")
    return pl.read_parquet(io.BytesIO(obj.get()["Body"].read()))


# --- Sidebar ---
st.sidebar.title("🌊 Financial Lakehouse")
st.sidebar.caption("Medallion Architecture · DuckDB · MinIO")

selected_label = st.sidebar.selectbox("View", list(VIEWS.keys()))
view_id = VIEWS[selected_label]

df = load_view(view_id)

tickers = sorted(df["ticker"].unique().to_list())
selected_tickers = st.sidebar.multiselect(
    "Tickers",
    tickers,
    default=tickers[:5] if len(tickers) >= 5 else tickers,
)

# --- Filter ---
if selected_tickers:
    df = df.filter(pl.col("ticker").is_in(selected_tickers))

st.title(selected_label)
st.caption(f"{len(df)} records · {df['ticker'].n_unique()} tickers · cache TTL 5min")

# --- KPI Row ---
col1, col2, col3, col4 = st.columns(4)

latest = (
    df.sort("datetime")
    .group_by("ticker")
    .last()
)

avg_return_1h = latest["return_1h"].drop_nulls().mean()
avg_return_24h = latest["return_24h"].drop_nulls().mean()
avg_volatility = latest["volatility_24h"].drop_nulls().mean()
total_volume = latest["volume"].drop_nulls().sum()

col1.metric("Avg Return 1h", f"{avg_return_1h:.2%}" if avg_return_1h else "N/A")
col2.metric("Avg Return 24h", f"{avg_return_24h:.2%}" if avg_return_24h else "N/A")
col3.metric("Avg Volatility 24h", f"{avg_volatility:.4f}" if avg_volatility else "N/A")
col4.metric("Total Volume", f"{total_volume:,.0f}" if total_volume else "N/A")

st.divider()

# --- Price Chart ---
st.subheader("📈 Price (USD)")
fig_price = px.line(
    df.to_pandas(),
    x="datetime",
    y="price_usd",
    color="ticker",
    template="plotly_dark",
)
fig_price.update_layout(height=400, margin=dict(l=0, r=0, t=0, b=0))
st.plotly_chart(fig_price, use_container_width=True)

col_left, col_right = st.columns(2)

# --- Volatility Ranking ---
with col_left:
    st.subheader("🌪️ Volatility Ranking (24h)")
    vol_rank = (
        latest.select(["ticker", "volatility_24h"])
        .drop_nulls()
        .sort("volatility_24h", descending=True)
    )
    fig_vol = px.bar(
        vol_rank.to_pandas(),
        x="volatility_24h",
        y="ticker",
        orientation="h",
        template="plotly_dark",
        color="volatility_24h",
        color_continuous_scale="Reds",
    )
    fig_vol.update_layout(height=400, margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
    st.plotly_chart(fig_vol, use_container_width=True)

# --- Returns 24h ---
with col_right:
    st.subheader("📊 Returns 24h")
    ret_rank = (
        latest.select(["ticker", "return_24h"])
        .drop_nulls()
        .sort("return_24h", descending=True)
    )
    colors = ["green" if r >= 0 else "red" for r in ret_rank["return_24h"].to_list()]
    fig_ret = go.Figure(go.Bar(
        x=ret_rank["return_24h"].to_list(),
        y=ret_rank["ticker"].to_list(),
        orientation="h",
        marker_color=colors,
    ))
    fig_ret.update_layout(
        height=400,
        margin=dict(l=0, r=0, t=0, b=0),
        template="plotly_dark",
        showlegend=False,
    )
    st.plotly_chart(fig_ret, use_container_width=True)

# --- PLN Price ---
if "price_pln" in df.columns:
    st.subheader("💱 Price (PLN)")
    fig_pln = px.line(
        df.to_pandas(),
        x="datetime",
        y="price_pln",
        color="ticker",
        template="plotly_dark",
    )
    fig_pln.update_layout(height=300, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig_pln, use_container_width=True)

# --- Raw Data ---
with st.expander("🔍 Raw Data"):
    st.dataframe(df.to_pandas(), use_container_width=True)