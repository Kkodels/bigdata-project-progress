import streamlit as st  
import pandas as pd  
 
import os

st.title(" 🚨 Real-Time Fraud Detection Dashboard")  
 
data_path = "stream_data/realtime_output/"

if os.path.exists(data_path) and any(f.endswith('.parquet') for f in os.listdir(data_path)):
    try:
        df = pd.read_parquet(data_path)  
        
        st.metric("Total Transaksi", len(df))  
        st.metric("Total Fraud", len(df[df["status"]=="FRAUD"]))  
        
        st.dataframe(df.tail(10))  
        st.bar_chart(df["status"].value_counts())
    except Exception as e:
        st.warning("Data Parquet sedang melangsungkan proses batch pertama...")
else:
    st.info("🕒 Menunggu Spark Streaming memproses dan membuat file data...")

import time
time.sleep(3)
st.rerun()
