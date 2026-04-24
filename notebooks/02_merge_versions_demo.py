from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.processing.merge_kpi_versions import merge_kpi_versions

spark = SparkSession.builder.getOrCreate()

# Example data
data = [
    ("cell_1", "v1", 1, "2026-01-01", 10),
    ("cell_1", "v2", 2, "2026-01-02", None),
    ("cell_1", "v3", 3, "2026-01-03", 20),
]

columns = ["Cell", "version", "priority", "ingestion_ts", "value"]

df = spark.createDataFrame(data, columns)

df = df.withColumn("ingestion_ts", F.to_timestamp("ingestion_ts"))

df_merged = merge_kpi_versions(
    df,
    key_cols=["Cell"],
    priority_col="priority",
    timestamp_col="ingestion_ts"
)

df_merged.show()
