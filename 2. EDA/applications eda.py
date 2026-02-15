from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr, count, when, countDistinct



sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()


RAW_PATH = "s3://steam-dataset-2025-bucket/raw/"


steam_csv_path = f"s3://steam-dataset-2025-bucket/Steam/steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/"


applications_raw_df = (
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


applications_selected_df = (
    applications_raw_df
    .select(
        col("appid").cast("long").alias("appid"),
        col("name"),
        col("type"),
        col("is_free").cast("boolean"),

        to_date(col("release_date")).alias("release_date"),
        expr("try_cast(required_age as int)").alias("required_age"),

        col("short_description"),
        col("supported_languages"),
        col("header_image"),
        col("background"),

        col("metacritic_score").cast("int"),
        col("recommendations_total").cast("long"),

        col("mat_supports_windows").cast("boolean"),
        col("mat_supports_mac").cast("boolean"),
        col("mat_supports_linux").cast("boolean"),

        col("mat_initial_price").cast("double"),
        col("mat_final_price").cast("double"),
        col("mat_discount_percent").cast("double"),
        col("mat_currency"),

        col("mat_achievement_count").cast("int"),

        col("mat_pc_os_min"),
        col("mat_pc_processor_min"),
        col("mat_pc_memory_min"),
        col("mat_pc_graphics_min"),

        col("mat_pc_os_rec"),
        col("mat_pc_processor_rec"),
        col("mat_pc_memory_rec"),
        col("mat_pc_graphics_rec"),

        col("created_at"),
        col("updated_at")
    )
)





# showing counts
applications_raw_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()


total_rows = applications_raw_df.count()

null_counts = applications_raw_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in applications_raw_df.columns
])

null_percentages = applications_raw_df.select([
    (count(when(col(c).isNull(), c)) / total_rows * 100).alias(c)
    for c in applications_raw_df.columns
])

null_counts.show(truncate=False)
null_percentages.show(truncate=False)


zero_summary = applications_raw_df.select(
    count(when(col("mat_initial_price") == 0, "mat_initial_price")).alias("initial_price_zero"),
    count(when(col("mat_final_price") == 0, "mat_final_price")).alias("final_price_zero"),
    count(when(col("mat_discount_percent") == 0, "mat_discount_percent")).alias("discount_zero"),
    count(when(col("recommendations_total") == 0, "recommendations_total")).alias("recommendations_zero")
)

zero_summary.show()


applications_raw_df.select(
    "mat_initial_price",
    "mat_final_price",
    "mat_discount_percent",
    "recommendations_total",
    "mat_achievement_count"
).summary("min", "50%", "90%", "95%", "99%", "max").show()


price_init_p99 = applications_raw_df.approxQuantile("mat_initial_price", [0.99], 0.01)[0]
price_final_p99 = applications_raw_df.approxQuantile("mat_final_price", [0.99], 0.01)[0]
reco_p99 = applications_raw_df.approxQuantile("recommendations_total", [0.99], 0.01)[0]
ach_p99 = applications_raw_df.approxQuantile("mat_achievement_count", [0.99], 0.01)[0]

print(price_init_p99,price_final_p99, reco_p99, ach_p99 )

applications_selected_df.write.mode("overwrite").csv(
    steam_csv_path
)
