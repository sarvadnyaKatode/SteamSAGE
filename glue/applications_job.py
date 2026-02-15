import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (col, when, countDistinct, count, lit, isnan, to_date, expr, year)


# Glue job arguments
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'RAW_BASE',    # Passed from Terraform
        'SILVER_BASE'  # Passed from Terraform
    ]
)

# Dynamic Paths (using the arguments passed by Terraform instead of hardcoding)
RAW_BASE = args['RAW_BASE']
SILVER_BASE = args['SILVER_BASE']

print(f"Starting Glue job: {args['JOB_NAME']}")
print(f"RAW_BASE: {RAW_BASE}")
print(f"SILVER_BASE: {SILVER_BASE}")

# Defining Input/Output paths based on dynamic bases
applications_input = f"{RAW_BASE}/applications.csv"
applications_out_parquet = f"{SILVER_BASE}/applications/bi_applications/"

# Glue Context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Reading raw applications
applications_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(applications_input)
)

# Base column selection
bi_applications_df = applications_df.select(
    "appid",
    "name",
    "type",
    "is_free",
    "mat_initial_price",
    "mat_final_price",
    "mat_discount_percent",
    "mat_currency",
    "metacritic_score",
    "recommendations_total",
    "mat_achievement_count",
    "mat_supports_windows",
    "mat_supports_mac",
    "mat_supports_linux",
    "release_date",
    "required_age"
)


# Cleaning metacritic_score (keeping numeric values only)
bi_applications_df = bi_applications_df.withColumn(
    "metacritic_score",
    col("metacritic_score").cast("double")
)

# Cleaning & capping required_age (max age upto 40)
bi_applications_df = bi_applications_df.withColumn(
    "required_age",
    when(col("required_age").cast("int").isNull(), None)
    .when(col("required_age").cast("int") > 40, 40)
    .otherwise(col("required_age").cast("int"))
)

# Cleaning & capping release_date (max year upto 2035)
bi_applications_df = bi_applications_df.withColumn(
    "release_date",
    when(to_date(col("release_date")).isNull(), None)
    .when(year(to_date(col("release_date"))) > 2035, lit("2035-12-31").cast("date"))
    .otherwise(to_date(col("release_date")))
)

# Cleaning & renaming discount percent (Only keeping values in range 0–100)
bi_applications_df = (
    bi_applications_df
    .withColumn(
        "discount_percent",
        when(col("mat_discount_percent").cast("double").isNull(), None)
        .when(col("mat_discount_percent").cast("double") < 0, 0)
        .when(col("mat_discount_percent").cast("double") > 100, 100)
        .otherwise(col("mat_discount_percent").cast("double"))
    )
    .drop("mat_discount_percent")
)

# Deriving release_year
bi_applications_df = bi_applications_df.withColumn(
    "release_year",
    when(col("release_date").isNull(), None)
    .otherwise(year(col("release_date")))
)

# Ensuring all values are of numeric types for quantile computation
bi_applications_df = (
    bi_applications_df
    .withColumn("mat_initial_price", col("mat_initial_price").cast("double"))
    .withColumn("mat_final_price", col("mat_final_price").cast("double"))
    .withColumn("recommendations_total", col("recommendations_total").cast("double"))
    .withColumn("mat_achievement_count", col("mat_achievement_count").cast("double"))
)

# Computing p99 thresholds
price_init_p99 = bi_applications_df.approxQuantile("mat_initial_price", [0.99], 0.01)[0]
price_final_p99 = bi_applications_df.approxQuantile("mat_final_price", [0.99], 0.01)[0]
reco_p99 = bi_applications_df.approxQuantile("recommendations_total", [0.99], 0.01)[0]
ach_p99 = bi_applications_df.approxQuantile("mat_achievement_count", [0.99], 0.01)[0]


# Applying p99 capping
bi_applications_capped_df = (
    bi_applications_df
    .withColumn(
        "mat_initial_price_capped",
        when(col("mat_initial_price") > price_init_p99, price_init_p99)
        .otherwise(col("mat_initial_price"))
    )
    .withColumn(
        "mat_final_price_capped",
        when(col("mat_final_price") > price_final_p99, price_final_p99)
        .otherwise(col("mat_final_price"))
    )
    .withColumn(
        "recommendations_total_capped",
        when(col("recommendations_total") > reco_p99, reco_p99)
        .otherwise(col("recommendations_total"))
    )
    .withColumn(
        "mat_achievement_count_capped",
        when(col("mat_achievement_count") > ach_p99, ach_p99)
        .otherwise(col("mat_achievement_count"))
    )
)

# Deriving prices
bi_applications_capped_df = bi_applications_capped_df.withColumn(
    "initial_price",
    when(col("mat_initial_price_capped").isNull() | isnan(col("mat_initial_price_capped")), None)
    .when(col("mat_currency") == "USD", col("mat_initial_price_capped") / 100)
    .otherwise(col("mat_initial_price_capped"))
)

bi_applications_capped_df = bi_applications_capped_df.withColumn(
    "final_price",
    when(col("mat_final_price_capped").isNull() | isnan(col("mat_final_price_capped")), None)
    .when(col("mat_currency") == "USD", col("mat_final_price_capped") / 100)
    .otherwise(col("mat_final_price_capped"))
)


# Deriving price_type
bi_applications_capped_df = bi_applications_capped_df.withColumn(
    "price_type",
    when(col("is_free") == True, "Free")
    .when(col("is_free") == False, "Paid")
    .otherwise(None)
)

# Deriving platform_support_type
bi_applications_capped_df = bi_applications_capped_df.withColumn(
    "platform_support_type",
    when(
        col("mat_supports_windows").isNull() |
        col("mat_supports_mac").isNull() |
        col("mat_supports_linux").isNull(),
        None
    )
    .when(
        (col("mat_supports_windows") == True) &
        (col("mat_supports_mac") == True) &
        (col("mat_supports_linux") == True),
        "Windows + Mac + Linux"
    )
    .when(
        (col("mat_supports_windows") == True) &
        (col("mat_supports_mac") == True),
        "Windows + Mac"
    )
    .when(
        (col("mat_supports_windows") == True) &
        (col("mat_supports_linux") == True),
        "Windows + Linux"
    )
    .when(col("mat_supports_windows") == True, "Windows Only")
    .when(col("mat_supports_mac") == True, "Mac Only")
    .when(col("mat_supports_linux") == True, "Linux Only")
    .otherwise("Other")
)

# Deriving price_band
bi_applications_capped_df = bi_applications_capped_df.withColumn(
    "price_band",
    when(col("price_type") == "Free", "Free")
    .when(col("final_price").isNull(), None)
    .when(col("final_price") == 0, "Free")
    .when(col("final_price") <= 10, "$1-$10")
    .when(col("final_price") <= 30, "$10-30")
    .when(col("final_price") <= 60, "$30-60")
    .otherwise("$60+")
)

# Writing output to SILVER (Parquet format)
bi_applications_capped_df.write.mode("overwrite").parquet(applications_out_parquet)




