from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr, count, when, countDistinct



sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()


RAW_PATH = "s3://steam-dataset-2025-bucket/raw/"


steam_csv_path = f"s3://steam-dataset-2025-bucket/Steam/steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/"


reviews_raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .load(RAW_PATH)
)


reviews_selected_df = (
    reviews_raw_df
    .select(
        col("recommendationid").cast("long"),
        col("appid").cast("long"),

        col("author_last_played").cast("long"),
        col("author_num_games_owned").cast("int"),
        col("author_num_reviews").cast("int"),

        col("author_playtime_at_review").cast("double"),
        col("author_playtime_forever").cast("long"),
        col("author_playtime_last_two_weeks").cast("long"),

        col("author_steamid"),

        col("comment_count").cast("int"),

        col("created_at"),
        col("updated_at"),

        col("embedding_run_id").cast("int"),

        col("language"),

        col("received_for_free").cast("boolean"),
        col("steam_purchase").cast("boolean"),
        col("written_during_early_access").cast("boolean"),

        col("timestamp_created").cast("long"),
        col("timestamp_updated").cast("long"),

        col("voted_up").cast("boolean"),

        col("votes_funny").cast("int"),
        col("votes_up").cast("int"),

        col("weighted_vote_score").cast("double")
    )
)

reviews_raw_df.select(
    countDistinct("recommendationid").alias("distinct_reviews"),
    count("*").alias("total_rows")
).show()


total_rows = reviews_raw_df.count()

null_counts = reviews_raw_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in reviews_raw_df.columns
])

null_percentages = reviews_raw_df.select([
    (count(when(col(c).isNull(), c)) / total_rows * 100).alias(c)
    for c in reviews_raw_df.columns
])

null_counts.show(truncate=False)
null_percentages.show(truncate=False)


reviews_raw_df.select(
    "author_playtime_forever",
    "author_playtime_at_review",
    "author_playtime_last_two_weeks",
    "votes_up",
    "votes_funny",
    "comment_count",
    "weighted_vote_score"
).summary("min", "50%", "90%", "95%", "99%", "max").show()

playtime_forever_p99 = reviews_raw_df.approxQuantile("author_playtime_forever", [0.99], 0.01)[0]
playtime_review_p99 = reviews_raw_df.approxQuantile("author_playtime_at_review", [0.99], 0.01)[0]
playtime_2w_p99 = reviews_raw_df.approxQuantile("author_playtime_last_two_weeks", [0.99], 0.01)[0]

votes_up_p99 = reviews_raw_df.approxQuantile("votes_up", [0.99], 0.01)[0]
votes_funny_p99 = reviews_raw_df.approxQuantile("votes_funny", [0.99], 0.01)[0]
comment_p99 = reviews_raw_df.approxQuantile("comment_count", [0.99], 0.01)[0]

print(playtime_forever_p99,playtime_review_p99,playtime_2w_p99,votes_up_p99,votes_funny_p99,comment_p99)


reviews_selected_df.write.mode("overwrite").csv(
    steam_csv_path
)


