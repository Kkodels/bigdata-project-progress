import streamlit as st
import pandas as pd
import os
import time

st.set_page_config(page_title="Real-Time E-Commerce Dashboard", layout="wide")
st.title("Real-Time E-Commerce Analytics Dashboard")

DATA_PATH = "data/serving/stream"
placeholder = st.empty()

def load_stream_data():
    if not os.path.exists(DATA_PATH): return pd.DataFrame()
    files = [f for f in os.listdir(DATA_PATH) if f.endswith(".parquet")]
    if not files: return pd.DataFrame()
    return pd.concat([pd.read_parquet(os.path.join(DATA_PATH, f)) for f in files], ignore_index=True)

while True:
    with placeholder.container():
        try:
            df = load_stream_data()
            if df.empty:
                st.info("Waiting for streaming data...")
                time.sleep(5)
                continue
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            st.subheader("Key Metrics")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Transactions", len(df))
            col2.metric("Total Revenue", f"${df['price'].sum():,.2f}")
            col3.metric("Avg Transaction", f"${df['price'].mean():,.2f}")
            col4.metric("Unique Cities", df["city"].nunique())
            
            st.divider()
            colA, colB = st.columns(2)
            with colA:
                st.subheader("Revenue by City")
                st.bar_chart(df.groupby("city")["price"].sum().sort_values(ascending=False))
            with colB:
                st.subheader("Top Products")
                st.bar_chart(df.groupby("product")["price"].sum().sort_values(ascending=False))
            
            st.divider()
            st.subheader("Revenue Trend")
            if "timestamp" in df.columns:
                st.line_chart(df.set_index("timestamp").resample("10s")["price"].sum())
            
            st.divider()
            st.subheader("Live Transactions")
            st.dataframe(df.sort_values("timestamp", ascending=False).head(30), use_container_width=True)
            
        except Exception as e:
            st.error(f"Streaming data not ready: {e}")
    time.sleep(5)
