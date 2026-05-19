from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
import os

def create_spark_session():
  
    spark = SparkSession.builder \
        .appName("SensorDataPipeline") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
        .config("spark.sql.streaming.fileSink.log.cleanupDelay", "10s") \
        .config("spark.sql.streaming.fileSink.log.compactInterval", "10") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def define_schema():
    return StructType([
        StructField("sensor_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("source", StringType(), True),
        StructField("is_anomaly", BooleanType(), True)
    ])

def process_kafka_stream(spark, bootstrap_servers, topic):
    kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), define_schema()).alias("data")) \
        .select("data.*")

    raw_df = parsed_df.withColumn("processing_time", current_timestamp())
    curated_df = raw_df.withColumn("value_category", 
                    when(col("sensor_type") == "temperature",
                         when(col("value") < 20, "LOW")
                         .when(col("value") > 35, "HIGH")
                         .otherwise("NORMAL"))
                    .when(col("sensor_type") == "humidity",
                         when(col("value") < 40, "LOW")
                         .when(col("value") > 80, "HIGH")
                         .otherwise("NORMAL"))
                    .when(col("sensor_type") == "pressure",
                         when(col("value") < 1000, "LOW")
                         .when(col("value") > 1020, "HIGH")
                         .otherwise("NORMAL"))
                    .otherwise("UNKNOWN"))
    consumption_df = curated_df.filter(col("is_anomaly") == True)

   
    raw_query = raw_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "file:///C:/tmp/datalake/raw") \
        .option("checkpointLocation", "./checkpoint/raw") \
        .option("cleanSource", "archive") \
        .option("sourceArchiveDir", "./archive/raw") \
        .start()

    curated_query = curated_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "file:///C:/tmp/datalake/curated") \
        .option("checkpointLocation", "./checkpoint/curated") \
        .option("cleanSource", "archive") \
        .option("sourceArchiveDir", "./archive/curated") \
        .start()

    consumption_query = consumption_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "file:///C:/tmp/datalake/consumption") \
        .option("checkpointLocation", "./checkpoint/consumption") \
        .option("cleanSource", "archive") \
        .option("sourceArchiveDir", "./archive/consumption") \
        .start()

   
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    
    os.makedirs("./archive/raw", exist_ok=True)
    os.makedirs("./archive/curated", exist_ok=True)
    os.makedirs("./archive/consumption", exist_ok=True)
    
    BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
    TOPIC = "sensor-events"
    spark = create_spark_session()
    try:
        process_kafka_stream(spark, BOOTSTRAP_SERVERS, TOPIC)
    except Exception as e:
        print(f"Stream error: {str(e)}")
    finally:
        spark.stop()