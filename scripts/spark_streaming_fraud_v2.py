from pyspark.sql import SparkSession  
from pyspark.sql.functions import *  
from pyspark.sql.types import *  
 
spark = SparkSession.builder.appName("FraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()  
 
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","bank_topic") \
    .load() 
 
schema = StructType([  
    StructField("nama", StringType()),  
    StructField("rekening", StringType()),  
    StructField("jumlah", IntegerType()),  
    StructField("lokasi", StringType())  
]) 
 
df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")  
 
# Masking  
df = df.withColumn("rekening_masked",  
    concat(lit("****"), col("rekening").substr(-2,2))) 
 
# Fraud detection  
df = df.withColumn("status",  
    when(col("jumlah") > 50000000, "FRAUD")  
    .when(col("lokasi") == "Luar Negeri", "FRAUD")  
    .otherwise("NORMAL"))  
 
# Encryption
df = df.withColumn("jumlah_encrypted",  
    base64(col("jumlah").cast("string")))  
 
query = df.writeStream \
    .format("parquet") \
    .option("path","stream_data/realtime_output/") \
    .option("checkpointLocation","data/checkpoints/") \
    .start()  
 
query.awaitTermination()
