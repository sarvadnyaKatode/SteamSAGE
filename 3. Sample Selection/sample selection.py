
masterdata_df = spark.read\
    .format("csv")\
    .option("header", True)\
    .load("/Volumes/workspace/default/rawdata/steam/processed/masterdata_exports/masterdata/")

from pyspark.sql.functions import col

filtered_master_df = (
    masterdata_df
    .filter(col("type") == "game")
    .filter(col("release_date").isNotNull())
)


print("Original masterdata rows:", masterdata_df.count())
print("After filtering (games + release_date):", filtered_master_df.count())


filtered_master_df.select("type").distinct().show()


filtered_master_df.select("release_date").summary("min", "max").show()


from pyspark.sql.functions import year

with_year_df = (
    filtered_master_df
    .withColumn("release_year", year("release_date"))
)


with_year_df.select("release_year").summary("min", "max").show()


from pyspark.sql.functions import current_date

current_year = year(current_date())

clean_year_df = (
    with_year_df
    .filter((col("release_year") >= 1995) & (col("release_year") <= current_year))
)


print("Rows before year cleanup:", with_year_df.count())
print("Rows after year cleanup:", clean_year_df.count())


clean_year_df.select("release_year").summary("min", "max").show()


clean_year_df.groupBy("release_year").count().orderBy("release_year").show(10)


clean_year_df.groupBy("release_year", "is_free").count() \
    .orderBy("release_year", "is_free") \
    .show(20)


from pyspark.sql.functions import count

stratum_counts_df = (
    clean_year_df
    .groupBy("release_year", "is_free")
    .agg(count("*").alias("stratum_count"))
)


with_stratum_df = (
    clean_year_df
    .join(
        stratum_counts_df,
        on=["release_year", "is_free"],
        how="left"
    )
)


with_stratum_df.select("stratum_count").summary("min", "50%", "90%", "max").show()


display(
    with_stratum_df
    .select("appid", "release_year", "is_free", "stratum_count")
    .limit(10)
)


from pyspark.sql.functions import rand

sampled_df = (
    with_stratum_df
    .withColumn(
        "keep_row",
        (with_stratum_df.stratum_count < 200) |
        (rand() < 0.20)
    )
    .filter("keep_row = true")
    .drop("keep_row", "stratum_count")
)


print("Final sampled rows:", sampled_df.count())


sampled_df.groupBy("release_year").count() \
    .orderBy("release_year") \
    .show(20)


sampled_df.groupBy("is_free").count().show()


display(
    sampled_df.select(
        "appid", "name", "release_year", "is_free",
        "genres", "platforms"
    ).limit(10)
)


sampled_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/visualization_sample/")