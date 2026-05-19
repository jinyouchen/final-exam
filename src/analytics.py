from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max
import os

# ======================
# Spark 初始化
# ======================
spark = SparkSession.builder \
    .appName("SensorAnalytics") \
    .master("local[*]") \
    .getOrCreate()

# ======================
# 关键修复：忽略流元数据，直接读 parquet 文件
# ======================
# 路径不变
CURATED_PATH = "C:/tmp/datalake/curated/domain=iot"
OUTPUT_DIR = "outputs/analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

try:
    # 直接读取文件，不读流元数据 → 修复报错
    df = spark.read.option("mergeSchema", "true").parquet(CURATED_PATH + "/*")
except:
    df = spark.read.option("mergeSchema", "true").parquet(CURATED_PATH)

print(" 读取数据成功")
df.printSchema()

# ======================
# 1. 总数量
# ======================
total = df.count()
print(f"\n===== 总记录数：{total} =====")

# ======================
# 2. 传感器统计
# ======================
print("\n===== 传感器统计 =====")
sensor_stats = df.groupBy("sensor") \
    .agg(
        count("*").alias("total"),
        avg("value").alias("avg"),
        min("value").alias("min"),
        max("value").alias("max"),
        sum(col("is_anomaly").cast("int")).alias("anomalies")
    )
sensor_stats.show(truncate=False)
sensor_stats.write.csv(OUTPUT_DIR + "/sensor_stats", header=True, mode="overwrite")

# ======================
# 3. 异常数据
# ======================
print("\n===== 异常数据 =====")
anomalies = df.filter(col("is_anomaly") == True)
anomalies.show(truncate=False)
anomalies.write.csv(OUTPUT_DIR + "/anomalies", header=True, mode="overwrite")

print("\n 分析完成！结果在 outputs/analytics")
spark.stop()