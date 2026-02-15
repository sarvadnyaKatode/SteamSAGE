import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import collect_set, concat_ws, col, trim, countDistinct, count
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Glue job arguments
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'RAW_BASE',    # Passed from Terraform
        'SILVER_BASE'  # Passed from Terraform
    ]
)

# Dynamic Paths
RAW_BASE = args['RAW_BASE']
SILVER_BASE = args['SILVER_BASE']

print(f"Starting Glue job: {args['JOB_NAME']}")
print(f"RAW_BASE: {RAW_BASE}")
print(f"SILVER_BASE: {SILVER_BASE}")

# Glue Context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Input paths (RAW)
app_devs_path       = f"{RAW_BASE}/application_developers.csv"
developers_path     = f"{RAW_BASE}/developers.csv"

app_publishers_path = f"{RAW_BASE}/application_publishers.csv"
publishers_path     = f"{RAW_BASE}/publishers.csv"

app_genres_path     = f"{RAW_BASE}/application_genres.csv"
genres_path         = f"{RAW_BASE}/genres.csv"

app_categories_path = f"{RAW_BASE}/application_categories.csv"
categories_path     = f"{RAW_BASE}/categories.csv"

app_platforms_path  = f"{RAW_BASE}/application_platforms.csv"
platforms_path      = f"{RAW_BASE}/platforms.csv"

# Output paths (SILVER)
out_devs       = f"{SILVER_BASE}/dimensions/app_developers/"
out_publishers = f"{SILVER_BASE}/dimensions/app_publishers/"
out_genres     = f"{SILVER_BASE}/dimensions/app_genres/"
out_categories = f"{SILVER_BASE}/dimensions/app_categories/"
out_platforms  = f"{SILVER_BASE}/dimensions/app_platforms/"

# Reading RAW CSVs
app_devs_df = spark.read.option("header", "true").csv(app_devs_path)
developers_df = spark.read.option("header", "true").csv(developers_path)

app_pubs_df = spark.read.option("header", "true").csv(app_publishers_path)
publishers_df = spark.read.option("header", "true").csv(publishers_path)

app_genres_df = spark.read.option("header", "true").csv(app_genres_path)
genres_df = spark.read.option("header", "true").csv(genres_path)

app_categories_df = spark.read.option("header", "true").csv(app_categories_path)
categories_df = spark.read.option("header", "true").csv(categories_path)

app_platforms_df = spark.read.option("header", "true").csv(app_platforms_path)
platforms_df = spark.read.option("header", "true").csv(platforms_path)

# DEVELOPERS (1 row per app per developer)
app_developers_rows_df = (
    app_devs_df
    .join(developers_df, app_devs_df.developer_id == developers_df.id, "left")
    .select(
        app_devs_df.appid.cast("long").alias("appid"),
        trim(col("name")).alias("developer")
    )
    .filter(col("developer").isNotNull())
    .dropDuplicates()
)

# PUBLISHERS
app_publishers_rows_df = (
    app_pubs_df
    .join(publishers_df, app_pubs_df.publisher_id == publishers_df.id, "left")
    .select(
        app_pubs_df.appid.cast("long").alias("appid"),
        trim(col("name")).alias("publisher")
    )
    .filter(col("publisher").isNotNull())
    .dropDuplicates()
)

# GENRES
app_genres_rows_df = (
    app_genres_df
    .join(genres_df, app_genres_df.genre_id == genres_df.id, "left")
    .select(
        app_genres_df.appid.cast("long").alias("appid"),
        trim(col("name")).alias("genre")
    )
    .filter(col("genre").isNotNull())
    .dropDuplicates()
)

# CATEGORIES
app_categories_rows_df = (
    app_categories_df
    .join(categories_df, app_categories_df.category_id == categories_df.id, "left")
    .select(
        app_categories_df.appid.cast("long").alias("appid"),
        trim(col("name")).alias("category")
    )
    .filter(col("category").isNotNull())
    .dropDuplicates()
)

# PLATFORMS
app_platforms_rows_df = (
    app_platforms_df
    .join(platforms_df, app_platforms_df.platform_id == platforms_df.id, "left")
    .select(
        app_platforms_df.appid.cast("long").alias("appid"),
        trim(col("name")).alias("platform")
    )
    .filter(col("platform").isNotNull())
    .dropDuplicates()
)

# Write dimension outputs to SILVER (Parquet format)
app_developers_rows_df.write.mode("overwrite").parquet(out_devs)
app_publishers_rows_df.write.mode("overwrite").parquet(out_publishers)
app_genres_rows_df.write.mode("overwrite").parquet(out_genres)
app_categories_rows_df.write.mode("overwrite").parquet(out_categories)
app_platforms_rows_df.write.mode("overwrite").parquet(out_platforms)

