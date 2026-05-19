from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, min, max, count, sum, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

KAFKA_BROKERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC = "sensor-events"
RAW_PATH = "C:/tmp/datalake/raw/source=kafka/topic=sensor-events"
CURATED_PATH = "C:/tmp/datalake/curated/domain=iot"
CONSUME_PATH = "C:/tmp/datalake/consumption/use_case=sensor_averages"
CKPT_RAW = "C:/tmp/ckpt_raw"
CKPT_CUR = "C:/tmp/ckpt_cur"
CKPT_CONS = "C:/tmp/ckpt_cons"

SCHEMA = StructType([
    StructField("sensor", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("timestamp", LongType()),
    StructField("source", StringType()),
    StructField("anomaly", BooleanType())
])

spark = SparkSession.builder \
    .appName("SensorPipeline") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", TOPIC) \
    .load()

# ----------------------- 最终修复：时间格式正确 -----------------------
raw_out = raw.select(
    col("value").cast("string").alias("raw_json"),
    col("timestamp").alias("ingest_time"),
    col("partition"), col("offset")
).withColumn("ingest_ts", to_timestamp(col("ingest_time"))) \
 .withColumn("year", year("ingest_ts")) \
 .withColumn("month", month("ingest_ts")) \
 .withColumn("day", dayofmonth("ingest_ts")) \
 .withColumn("hour", hour("ingest_ts"))

q_raw = raw_out.writeStream \
    .format("json") \
    .option("path", RAW_PATH) \
    .option("checkpointLocation", CKPT_RAW) \
    .partitionBy("year", "month", "day", "hour") \
    .start()

# Parse
parsed = raw.select(
    from_json(col("value").cast("string"), SCHEMA).alias("data")
).select("data.*").withColumn("event_time", to_timestamp(col("timestamp")))

# Curated with partitions
curated = parsed.withColumn("is_anomaly",
    (col("sensor") == "temperature") & (col("value") > 35) |
    (col("sensor") == "humidity") & (col("value") > 90) |
    (col("sensor") == "pressure") & (col("value") < 990) |
    (col("sensor") == "pressure") & (col("value") > 1030)
).filter(col("value").isNotNull()) \
 .withColumn("year", year("event_time")) \
 .withColumn("month", month("event_time")) \
 .withColumn("day", dayofmonth("event_time"))

q_cur = curated.writeStream \
    .format("parquet") \
    .option("path", CURATED_PATH) \
    .option("checkpointLocation", CKPT_CUR) \
    .partitionBy("sensor", "year", "month", "day") \
    .start()

# Aggregation
agg = curated.withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "5 minutes"), col("sensor")) \
    .agg(
        avg("value").alias("avg"),
        min("value").alias("min"),
        max("value").alias("max"),
        count("value").alias("count"),
        sum(col("is_anomaly").cast("int")).alias("anomaly_count")
    )

q_agg = agg.writeStream \
    .format("parquet") \
    .option("path", CONSUME_PATH) \
    .option("checkpointLocation", CKPT_CONS) \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()