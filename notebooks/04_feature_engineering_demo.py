from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.features.feature_engineering_4g import add_time_features

spark = SparkSession.builder.getOrCreate()

data = [
    ("cell_1", "2026-01-01 00:00:00", 10.0),
    ("cell_2", "2026-01-03 12:00:00", 25.0),
]

cols = ["Cell", "DATETIME_ID", "RRC_connected_users"]

df = spark.createDataFrame(data, cols)
df = df.withColumn("DATETIME_ID", F.to_timestamp("DATETIME_ID"))

df_features = add_time_features(df)

df_features.show()
