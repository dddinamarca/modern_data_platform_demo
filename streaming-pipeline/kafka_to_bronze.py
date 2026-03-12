from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("StreamingPipeline") \
    .getOrCreate()

schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("product", StringType()) \
    .add("price", IntegerType()) \
    .add("quantity", IntegerType()) \
    .add("event_time", LongType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","sales_events") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"),schema).alias("data")) \
    .select("data.*")

query = parsed.writeStream \
    .format("delta") \
    .option("checkpointLocation","/tmp/checkpoints") \
    .start("lakehouse/bronze/sales")

query.awaitTermination()
