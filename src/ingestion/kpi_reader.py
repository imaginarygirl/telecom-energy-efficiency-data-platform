from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def read_csv_with_metadata(spark, paths: list[str]) -> DataFrame:
    """
    Generic CSV reader with source metadata.
    """
    return (
        spark.read
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(paths)
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_ts", F.current_timestamp())
    )


def align_to_columns(df: DataFrame, target_cols: list[str]) -> DataFrame:
    """
    Align a DataFrame to a target schema by column name.
    Missing columns are added as null.
    Extra columns are ignored.
    """
    for col_name in target_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None))

    return df.select(*target_cols)
