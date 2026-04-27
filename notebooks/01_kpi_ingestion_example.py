from pyspark.sql import SparkSession
from src.ingestion.kpi_reader import read_csv_with_metadata, align_to_columns

spark = SparkSession.builder.getOrCreate()

paths = ["data/sample_kpi_v1.csv", "data/sample_kpi_v2.csv"]

target_cols = [
    "DATETIME_ID",
    "Site",
    "RadioNode",
    "Cell",
    "DL_data_volume",
    "UL_data_volume",
    "RRC_connected_users",
    "source_file",
    "ingestion_ts",
]

df_raw = read_csv_with_metadata(spark, paths)
df_aligned = align_to_columns(df_raw, target_cols)

df_aligned.show(truncate=False)
df_aligned.printSchema()
