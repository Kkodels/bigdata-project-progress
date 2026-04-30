# ==========================================
# DASHBOARD STREAMLIT (STABLE)
# ==========================================

import streamlit as st
from pyspark.sql import SparkSession
import plotly.express as px
import pandas as pd
from sklearn.linear_model import LinearRegression
import os

# ==========================================
# DYNAMIC PATH CONFIG
# ==========================================
# Mencari lokasi folder output secara otomatis
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# Mengatur judul dan lebar tampilan web
st.set_page_config(page_title="Traffic Dashboard", layout="wide")
st.title("Smart City AI Traffic Dashboard")

# ==========================================
# INIT SPARK
# ==========================================
@st.cache_resource
def get_spark():
    return SparkSession.builder.appName("Dashboard_App").getOrCreate()

spark = get_spark()

# ==========================================
# DATA LOADING WITH ERROR HANDLING
# ==========================================
def load_parquet(folder_name):
    path = os.path.join(OUTPUT_DIR, folder_name)
    if not os.path.exists(path):
        st.error(f"Folder data {folder_name} tidak ditemukan! Jalankan main script dulu.")
        st.stop()
    return spark.read.parquet(path).toPandas()

# Mengambil 3 jenis data: Total, Tren Waktu, dan Data untuk AI
try:
    pdf = load_parquet("traffic")
    pdf_time = load_parquet("traffic_time")
    pdf_ml = load_parquet("ml_data")
except Exception as e:
    st.error(f"Gagal memuat data: {e}")
    st.stop()

# ==========================================
# SIDEBAR & FILTER
# ==========================================
locations = pdf["location"].unique()
# Membuat pilihan lokasi di samping kiri (sidebar)
selected_loc = st.sidebar.selectbox("Pilih Lokasi Analisis", locations)

# Menyaring data hanya untuk lokasi yang dipilih user
filtered_pdf = pdf[pdf["location"] == selected_loc]

# ==========================================
# KPI METRICS
# ==========================================
st.subheader("Key Performance Indicators")
col1, col2 = st.columns(2)

with col1:
    st.metric("Total Kendaraan (All)", int(pdf["total_vehicle"].sum()))
with col2:
    st.metric(f"Total di {selected_loc}", int(filtered_pdf["total_vehicle"].sum()))

# ==========================================
# VISUALIZATION & PREDICTION
# ==========================================
st.markdown("---")
c1, c2 = st.columns(2)

with c1:
    st.subheader("Traffic Time Series")
    # Memperbaiki format waktu agar bisa dibaca grafik
    pdf_time["start_time"] = pdf_time["window"].apply(lambda x: x[0] if isinstance(x, tuple) else x.start)
    
    # Membuat grafik garis menggunakan Plotly
    fig_line = px.line(pdf_time, x="start_time", y="total_vehicle", color="location")
    st.plotly_chart(fig_line, use_container_width=True)

with c2:
    st.subheader("AI Prediction (Linear Regression)")
    # Menyiapkan data untuk dipelajari AI
    X = pdf_ml[["hour"]]
    y = pdf_ml["vehicle_count"]
    
    # Melatih AI secara instan
    model = LinearRegression()
    model.fit(X, y)
    
    # Membuat slider agar user bisa pilih jam (misal jam 5 sore)
    hour_input = st.slider("Prediksi Jam Ke-", 0, 23, 12)
    pred = model.predict([[hour_input]])
    st.success(f"Prediksi jumlah kendaraan pada jam {hour_input}:00 adalah **{int(pred[0])}**")