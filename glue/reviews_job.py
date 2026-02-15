import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import (col, when, from_unixtime, to_date, year, month, date_format, countDistinct, count, trim)
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Glue job arguments
args = getResolvedOptions(
    sys.argv, 
    [
        'JOB_NAME',
        'RAW_BASE',     # Old Bucket (Raw)
        'SILVER_BASE',  # New Bucket (Output)
        'SCORES_BASE'   # Old Bucket (Silver/Scores)
    ]
)

# Dynamic Paths
RAW_BASE    = args['RAW_BASE']
SILVER_BASE = args['SILVER_BASE']
SCORES_BASE = args['SCORES_BASE']

print(f"Starting Glue job: {args['JOB_NAME']}")
print(f"RAW_BASE (Input): {RAW_BASE}")
print(f"SCORES_BASE (Input): {SCORES_BASE}")
print(f"SILVER_BASE (Output): {SILVER_BASE}")

# Glue Context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Input / Output paths
reviews_input = f"{RAW_BASE}/reviews.csv"

# Using SCORES_BASE (Old Bucket) to find the file
review_score_input = f"{SCORES_BASE}/reviews/reviews_scored_final.csv"

# Output goes to SILVER_BASE (New Bucket)
review_out_parquet = f"{SILVER_BASE}/reviews/bi_reviews/"

# Reading RAW reviews
reviews_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .load(reviews_input)
)

# Selecting required columns
bi_reviews_df = reviews_df.select(
    "recommendationid",
    "appid",
    "votes_up",
    "votes_funny",
    "weighted_vote_score",
    "author_playtime_at_review",
    "author_num_reviews",
    "language",
    "timestamp_created"
)

# Converting playtime (minutes → hours → capped)
PLAYTIME_HOURS_P95 = 67.6  #calculated in EDA

bi_reviews_df = (
    bi_reviews_df
    .withColumn("author_playtime_at_review_hours", col("author_playtime_at_review") / 60.0)
    .withColumn(
        "author_playtime",
        when(col("author_playtime_at_review_hours") > PLAYTIME_HOURS_P95, PLAYTIME_HOURS_P95)
        .otherwise(col("author_playtime_at_review_hours"))
    )
)

# Engagement metric
bi_reviews_df = bi_reviews_df.withColumn(
    "review_reactions",
    col("votes_up") + col("votes_funny")
)

# Time dimensions
bi_reviews_df = (
    bi_reviews_df
    .withColumn("review_timestamp", from_unixtime(col("timestamp_created")))
    .withColumn("review_date", to_date(col("review_timestamp")))
    .withColumn("review_year", year(col("review_timestamp")))
)

# Droping intermediates
bi_reviews_df = bi_reviews_df.drop(
    "votes_up",
    "votes_funny",
    "author_playtime_at_review",
    "author_playtime_at_review_hours",
    "timestamp_created",
    "review_timestamp"
)

# Reading SENTIMENT FILE  
review_score_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(review_score_input)
    .withColumnRenamed("category", "review_category") 
)

# Joining sentiment data
review_fact_df = (
    bi_reviews_df
    .join(
        review_score_df,
        on="recommendationid",
        how="left"
    )
)

# Cleaning blanks → NULL
for c in ["language", "review_category"]:
    review_fact_df = review_fact_df.withColumn(
        c,
        when(trim(col(c)) == "", None).otherwise(col(c))
    )

# Final projection
review_fact_df = review_fact_df.select(
    "recommendationid",
    "appid",
    "review_date",
    "review_year",
    "review_reactions",
    "weighted_vote_score",
    "author_playtime",
    "author_num_reviews",
    "language",
    "review_category",
    "numeric_score"
)

# Writing output to SILVER (Parquet format)
review_fact_df.write.mode("overwrite").parquet(review_out_parquet)




