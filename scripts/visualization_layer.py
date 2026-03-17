# =====================================
# VISUALIZATION LAYER
# =====================================

from pyspark.sql import SparkSession
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

# ============================
# BASE DIR
# ============================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ============================
# HADOOP HOME SETUP (Windows)
# ============================
os.environ["HADOOP_HOME"] = os.path.join(BASE_DIR, "hadoop")
os.environ["PATH"] = os.path.join(BASE_DIR, "hadoop", "bin") + ";" + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("VisualizationLayer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(os.path.join(BASE_DIR, "data", "clean", "parquet"))

# Create reports folder
reports_dir = os.path.join(BASE_DIR, "reports")
if not os.path.exists(reports_dir):
    os.makedirs(reports_dir)

# Revenue per Category
category_df = df.groupBy("category") \
    .sum("total_amount") \
    .toPandas()

category_df = category_df.sort_values("sum(total_amount)", ascending=False)

plt.figure(figsize=(8,5))
plt.bar(category_df["category"], category_df["sum(total_amount)"])
plt.xticks(rotation=45)
plt.title("Revenue per Category")
plt.ylabel("Total Revenue")
plt.tight_layout()
plt.savefig(os.path.join(reports_dir, "category_revenue.png"))

print("Visualization saved to reports/category_revenue.png")

spark.stop()
