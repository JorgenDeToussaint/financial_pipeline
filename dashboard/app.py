import streamlit as st
import polars as pl
import boto3
import io
import os
import plotly.express as px
import plotly.graph_objects as go

# --- 0. Configuration (MUSI BYĆ NA POCZĄTKU) ---
st.set_page_config(
    page_title="Financial Lakehouse",
    page_icon="🌊",
    layout="wide"
)

# Definicja widoków (jeśli nie masz jej w osobnym configu)
VIEWS = {
    "🌍 Macro Overview": "macro_overview",
    "🛡️ Defense Tracker": "defense_tracker",
    "⛏️ Commodities": "commodities",
    "💾 Semiconductors": "semiconductors",
    "🪙 Crypto & DeFi": "crypto_defi",
    "🔭 Full Universe": "full_universe",
}

# --- 1. Resource Caching ---
@st.cache_resource
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    )

# --- 2. Data Caching ---
@st.cache_data(ttl=300)
def fetch_parquet(view_id: str) -> pl.DataFrame:
    client = get_s3_client()
    try:
        response = client.get_object(Bucket="gold", Key=f"views/{view_id}/data_daily.parquet")
        return pl.read_parquet(io.BytesIO(response["Body"].read()))
    except Exception as e:
        st.error(f"Error fetching data for {view_id}: {e}")
        return pl.DataFrame()

# --- 3. Sidebar & Logic ---
st.sidebar.title("🌊 Financial Lakehouse")
st.sidebar.caption("Medallion Architecture · Polars · MinIO")

selected_label = st.sidebar.selectbox("View", list(VIEWS.keys()))
view_id = VIEWS[selected_label]

df = fetch_parquet(view_id)

if df.is_empty():
    st.warning("No data found in S3 for this view.")
    st.stop()

# Optymalizacja filtracji tickerów
tickers = df["ticker"].unique().sort().to_list()
selected_tickers = st.sidebar.multiselect("Tickers", tickers, default=tickers[:5])

if selected_tickers:
    df = df.filter(pl.col("ticker").is_in(selected_tickers))

# Wyciąganie najnowszych rekordów (bez kosztownego sortowania całego DF)
latest = df.unique(subset="ticker", keep="last")

# --- 4. Main UI ---
st.title(selected_label)
st.caption(f"{len(df)} records · {df['ticker'].n_unique()} tickers · cache TTL 5min")

# Agregacja metryk w jednym przebiegu (One-pass aggregation)
metrics = latest.select([
    pl.col("return_1h").mean().alias("avg_ret_1h"),
    pl.col("return_24h").mean().alias("avg_ret_24h"),
    pl.col("volatility_24h").mean().alias("avg_vol"),
    pl.col("volume").sum().alias("total_vol")
]).to_dicts()[0]

# --- KPI Row (Twoje "itd") ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Avg Return 1h", f"{metrics['avg_ret_1h']:.2%}" if metrics['avg_ret_1h'] else "N/A")
col2.metric("Avg Return 24h", f"{metrics['avg_ret_24h']:.2%}" if metrics['avg_ret_24h'] else "N/A")
col3.metric("Avg Volatility", f"{metrics['avg_vol']:.4f}" if metrics['avg_vol'] else "N/A")
col4.metric("Total Volume", f"{metrics['total_vol']:,.0f}" if metrics['total_vol'] else "N/A")

st.divider()

# --- Charts ---
st.subheader("📈 Price (USD)")
# Plotly najlepiej radzi sobie z Pandasem przy dużych wykresach, 
# ale robimy to tylko dla danych potrzebnych do wykresu (.select)
fig_price = px.line(
    df.select(["datetime", "price_usd", "ticker"]).to_pandas(),
    x="datetime",
    y="price_usd",
    color="ticker",
    template="plotly_dark",
)
fig_price.update_layout(height=400, margin=dict(l=0, r=0, t=20, b=0))
st.plotly_chart(fig_price, use_container_width=True)

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🌪️ Volatility Ranking (24h)")
    vol_rank = latest.select(["ticker", "volatility_24h"]).drop_nulls().sort("volatility_24h", descending=True)
    fig_vol = px.bar(
        vol_rank.to_pandas(),
        x="volatility_24h", y="ticker",
        orientation="h", template="plotly_dark",
        color="volatility_24h", color_continuous_scale="Reds",
    )
    fig_vol.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig_vol, use_container_width=True)

with col_right:
    st.subheader("📊 Returns 24h")
    ret_rank = latest.select(["ticker", "return_24h"]).drop_nulls().sort("return_24h", descending=True)
    colors = ["green" if r >= 0 else "red" for r in ret_rank["return_24h"]]
    fig_ret = go.Figure(go.Bar(
        x=ret_rank["return_24h"], y=ret_rank["ticker"],
        orientation="h", marker_color=colors,
    ))
    fig_ret.update_layout(height=400, template="plotly_dark", showlegend=False)
    st.plotly_chart(fig_ret, use_container_width=True)

# --- Raw Data ---
with st.expander("🔍 Raw Data Explorer"):
    st.dataframe(df.to_pandas(), use_container_width=True)