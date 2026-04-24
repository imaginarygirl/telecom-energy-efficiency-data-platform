from pyspark.sql import functions as F


def null_count(df, cols):
    return df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in cols
    ])


def duplicate_keys(df, key_cols):
    return (
        df.groupBy(*key_cols)
        .count()
        .filter(F.col("count") > 1)
    )
