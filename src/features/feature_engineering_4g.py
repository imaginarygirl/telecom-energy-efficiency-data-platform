from pyspark.sql import functions as F


def add_time_features(df):
    return (
        df.withColumn("hour", F.hour("DATETIME_ID"))
          .withColumn("day_of_week", F.dayofweek("DATETIME_ID"))
          .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
    )
