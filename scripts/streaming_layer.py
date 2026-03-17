from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StreamingPipeline").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = "user_id INT, product STRING, price DOUBLE, city STRING, timestamp STRING"

stream_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("stream_data")

# 🔥 fungsi untuk handle tiap batch
def process_batch(df, epoch_id):
    print(f"\n=== Batch {epoch_id} ===")
    df.show(truncate=False)  # tampil ke terminal
    
    # simpan ke parquet
    df.write.mode("append").parquet("data/serving/stream")

query = stream_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()