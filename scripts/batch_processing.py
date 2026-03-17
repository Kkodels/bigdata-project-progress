# =====================================
# BATCH PROCESSING LAYER
# CSV -> Clean Parquet
# =====================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as _round, to_date, coalesce
from pyspark.sql.types import DoubleType, IntegerType
import os
import time

# ============================
# BASE DIR
# ============================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ============================
# HADOOP HOME SETUP (Windows)
# ============================
os.environ["HADOOP_HOME"] = os.path.join(BASE_DIR, "hadoop")
os.environ["PATH"] = os.path.join(BASE_DIR, "hadoop", "bin") + ";" + os.environ.get("PATH", "")

# ============================
# START TIMER
# ============================
start_time = time.time()

print("========================================")
print("     BATCH PROCESSING LAYER STARTED     ")
print("========================================")

# ============================
# INIT SPARK
# ============================
spark = SparkSession.builder \
    .appName("BatchProcessing") \
    .master("local[*]") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================
# LOAD RAW DATA
# ============================
print("Loading Raw CSV Data...")
df_raw = spark.read.option("header", True).csv(os.path.join(BASE_DIR, "ecommerce_raw.csv"))

total_raw = df_raw.count()
print(f"Total Raw Records: {total_raw}")
print("Schema:")
df_raw.printSchema()
print("Sample Data:")
df_raw.show(5)
print("----------------------------------------")

# ============================
# DATA CLEANING
# ============================
print("Cleaning Data...")

# Step 1: Cast data types
df_clean = df_raw \
    .withColumn("price", col("price").cast(DoubleType())) \
    .withColumn("quantity", col("quantity").cast(IntegerType())) \
    .withColumn("transaction_date", coalesce(
        to_date(col("transaction_date"), "yyyy-MM-dd"),
        to_date(col("transaction_date"), "dd-MM-yyyy")
    ))

# Step 2: Drop rows with null values
print("Dropping null rows...")
df_clean = df_clean.dropna()

# Step 3: Remove duplicate rows
before_dedup = df_clean.count()
df_clean = df_clean.dropDuplicates()
after_dedup = df_clean.count()
print(f"Duplicate rows removed: {before_dedup - after_dedup}")

# ============================
# COMPUTE TOTAL AMOUNT
# ============================
print("Computing total_amount = price * quantity...")
df_clean = df_clean.withColumn(
    "total_amount",
    _round(col("price") * col("quantity"), 2)
)

total_clean = df_clean.count()
print(f"Total Clean Records: {total_clean}")
print(f"Total Dropped Records: {total_raw - total_clean}")
print("Clean Schema:")
df_clean.printSchema()
print("Sample Clean Data:")
df_clean.show(5)
print("----------------------------------------")

# ============================
# SAVE AS PARQUET
# ============================
print("Saving Clean Data as Parquet...")

output_path = os.path.join(BASE_DIR, "data", "clean", "parquet")
clean_dir = os.path.join(BASE_DIR, "data", "clean")
if not os.path.exists(clean_dir):
    os.makedirs(clean_dir)

df_clean.write.mode("overwrite").parquet(output_path)

print(f"Clean data saved to {output_path}")
print("----------------------------------------")

# ============================
# STOP SPARK
# ============================
spark.stop()

end_time = time.time()
execution_time = round(end_time - start_time, 2)

print("========================================")
print("  BATCH PROCESSING COMPLETED SUCCESS   ")
print(f"   Execution Time: {execution_time} sec")
print("========================================")
