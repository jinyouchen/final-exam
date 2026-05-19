from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, min, count, when, current_timestamp,
    unix_timestamp, window, countDistinct, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType, IntegerType, LongType
)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("SensorAnalytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()
    return spark

def load_curated_data(spark, path="file:///tmp/datalake/curated"):
    print("Loading curated layer data...")
    
    schema = StructType([
        StructField("sensor_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("source", StringType(), True),
        StructField("is_anomaly", BooleanType(), True),
        StructField("value_category", StringType(), True),
        StructField("processing_time", TimestampType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("anomaly_flag", IntegerType(), True)
    ])
    
    df = spark.read \
        .schema(schema) \
        .parquet(path)
    
    df = df.withColumn("timestamp", col("timestamp").cast(DoubleType()))
    df = df.withColumn("event_time", from_unixtime(col("timestamp") / 1000).cast(TimestampType()))
    
    df.printSchema()
    print(f"Loaded row count: {df.count()}")
    
    return df

def basic_analytics(df):
    print("Executing basic sensor analytics...")
    
    stats_df = df.groupBy("sensor_type", "source") \
        .agg(
            count("*").alias("total_records"),
            avg("value").alias("avg_value"),
            max("value").alias("max_value"),
            min("value").alias("min_value"),
            count(when(col("is_anomaly") == True, 1)).alias("anomaly_count"),
            (count(when(col("is_anomaly") == True, 1)) / count("*")).alias("anomaly_rate"),
            current_timestamp().alias("analysis_time")
        )
    
    stats_df.show(truncate=False)
    return stats_df

def windowed_anomaly_analytics(df):
    print("Executing windowed anomaly analytics...")
    
    df = df.withColumn("event_time", col("event_time").cast(TimestampType()))
    
    windowed_stats_df = df.groupBy(
        "sensor_type", 
        "source",
        window(col("event_time"), "10 minutes", "5 minutes")
    ).agg(
        count("*").alias("window_records"),
        count(when(col("is_anomaly") == True, 1)).alias("window_anomaly_count"),
        avg("value").alias("window_avg_value"),
        max("value").alias("window_max_value"),
        min("value").alias("window_min_value")
    ).orderBy("sensor_type", "window")
    
    windowed_stats_df.show(10, truncate=False)
    return windowed_stats_df

def main():
    try:
        spark = create_spark_session()
        
        curated_df = load_curated_data(spark)
        
        basic_analytics(curated_df)
        
        windowed_anomaly_analytics(curated_df)
        
    except Exception as e:
        print(f"Analytics task failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()