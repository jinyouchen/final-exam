import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class LakeUtils:
    def __init__(self, base_path="C:/tmp/datalake"):
        self.base = base_path
        self.raw = f"{base_path}/raw"
        self.curated = f"{base_path}/curated"
        self.consumption = f"{base_path}/consumption"

    def ensure_dirs(self):
        os.makedirs(self.raw, exist_ok=True)
        os.makedirs(self.curated, exist_ok=True)
        os.makedirs(self.consumption, exist_ok=True)
        print(" Data lake directories ready")

    def read_curated(self, spark):
        path = f"{self.curated}/domain=iot"
        return spark.read.parquet(path)

    def read_consumption(self, spark):
        path = f"{self.consumption}/use_case=sensor_averages"
        return spark.read.parquet(path)

    def get_anomalies(self, df):
        return df.filter(col("is_anomaly") == True)