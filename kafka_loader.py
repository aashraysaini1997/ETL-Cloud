# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movielog6") \
    .option("startingOffsets", "earliest") \
    .option("spark.streaming.kafka.maxRatePerPartition", "50") \
    .load()

# COMMAND ----------

value_str = kafka_df.selectExpr("CAST(value AS STRING)").alias("value_str")
filtered_df = value_str.filter(col("value").like("%=%"))
display(filtered_df)

# COMMAND ----------

output_query = (
    filtered_df
    .writeStream
    .format("csv")
    .option("path", "s3a://aashraybuckets/ratings.csv/")
    .option("checkpointLocation", "s3a://aashraybuckets/ratings.csv/")
    .start()
)

output_query.awaitTermination(600)
