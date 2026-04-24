from pyspark.sql import functions as F
from pyspark.sql.window import Window


def merge_kpi_versions(df, key_cols, priority_col, timestamp_col):
    """
    Merge multiple KPI versions using priority and timestamp logic.

    Parameters:
        df: input DataFrame
        key_cols: list of columns defining uniqueness
        priority_col: version priority column (higher = newer)
        timestamp_col: ingestion timestamp

    Returns:
        Deduplicated DataFrame with latest records
    """

    window = (
        Window.partitionBy(*key_cols)
        .orderBy(F.col(priority_col).desc(), F.col(timestamp_col).desc())
    )

    return (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )
