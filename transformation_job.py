from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Define Schema (Schema Validation)
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("event", StringType()) \
    .add("product_id", IntegerType()) \
    .add("timestamp", TimestampType())

spark = SparkSession.builder.appName("EcommerceStreaming").getOrCreate()

# 1. Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream") \
    .load()

# 2. Transform & Validate
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 3. Handle Late Data (Watermarking)
enriched_df = json_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "1 minute"), col("event")) \
    .count()

# 4. Write to S3/Cloud Storage (Data Partitioning)
query = enriched_df.writeStream \
    .format("parquet") \
    .option("path", "s3://my-ecommerce-bucket/transformed_data/") \
    .option("checkpointLocation", "s3://my-ecommerce-bucket/checkpoints/") \
    .partitionBy("event") \
    .start()

query.awaitTermination()
