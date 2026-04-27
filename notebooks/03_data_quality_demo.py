from pyspark.sql import SparkSession
from src.quality.data_quality_checks import null_count, duplicate_keys

spark = SparkSession.builder.getOrCreate()

data = [
    ("cell_1", "2026-01-01 00:00:00", 10.0),
    ("cell_1", "2026-01-01 00:00:00", 10.0),
    ("cell_2", "2026-01-01 00:00:00", None),
]

cols = ["Cell", "DATETIME_ID", "RRC_connected_users"]

df = spark.createDataFrame(data, cols)

null_count(df, ["Cell", "DATETIME_ID", "RRC_connected_users"]).show()

duplicate_keys(df, ["Cell", "DATETIME_ID"]).show()
