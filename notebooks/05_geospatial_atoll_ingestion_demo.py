from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Simplified example of Atoll/site coordinates after parsing KML
data = [
    ("SITE_A", 14.49917, 50.05416),
    ("SITE_B", 14.53523, 50.06615),
]

df_sites = spark.createDataFrame(data, ["site", "longitude", "latitude"])

df_sites = df_sites.withColumn(
    "geometry_wkt",
    F.concat(
        F.lit("POINT ("),
        F.col("longitude"),
        F.lit(" "),
        F.col("latitude"),
        F.lit(")")
    )
)

df_sites.show(truncate=False)

# Simplified example of coverage polygons after shapefile parsing
coverage_data = [
    ("CELL_A", "POLYGON ((14.49 50.05, 14.50 50.05, 14.50 50.06, 14.49 50.06, 14.49 50.05))"),
    ("CELL_B", "POLYGON ((14.51 50.05, 14.52 50.05, 14.52 50.06, 14.51 50.06, 14.51 50.05))"),
]

df_coverage = spark.createDataFrame(coverage_data, ["cell", "geometry_wkt"])

df_coverage.show(truncate=False)
