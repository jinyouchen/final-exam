from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, when

class LakeUtils:
    def __init__(self, curated_path="file:///tmp/datalake/curated"):
        self.spark = SparkSession.builder \
            .appName("SensorLakeUtils") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.curated_path = curated_path

    def get_sensor_list(self):
        df = self.spark.read.parquet(self.curated_path)
        return df.select("sensor_type").distinct().rdd.map(lambda x: x[0]).collect()

    def get_latest_reading(self, sensor_type):
        df = self.spark.read.parquet(self.curated_path)
        latest = df.filter(col("sensor_type") == sensor_type) \
            .orderBy(col("timestamp").desc()) \
            .limit(1) \
            .toPandas()
        return latest.iloc[0].to_dict() if not latest.empty else None

    def get_sensor_stats(self, sensor_type):
        df = self.spark.read.parquet(self.curated_path)
        stats = df.filter(col("sensor_type") == sensor_type) \
            .agg(
                count("*").alias("total_records"),
                avg("value").alias("avg_value"),
                max("value").alias("max_value"),
                min("value").alias("min_value"),
                count(when(col("is_anomaly") == True, 1)).alias("anomaly_count")
            ) \
            .toPandas()
        return stats.iloc[0].to_dict() if not stats.empty else None

    def get_anomalies(self, sensor_type=None, limit=10):
        df = self.spark.read.parquet(self.curated_path).filter(col("is_anomaly") == True)
        if sensor_type:
            df = df.filter(col("sensor_type") == sensor_type)
        anomalies = df.select("sensor_type", "value", "unit", "timestamp", "source") \
            .orderBy(col("timestamp").desc()) \
            .limit(limit) \
            .toPandas()
        return anomalies.to_dict("records")