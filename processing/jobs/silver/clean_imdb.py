"""
Silver Layer: Clean IMDb Data

Transformations:
- Filter to titleType='movie' only
- Normalize tconst format (ensure tt prefix)
- Cast numeric fields (startYear, runtimeMinutes, averageRating, numVotes)
- Split genres from comma-separated to array

Input: s3://movies-datalake-2310/bronze/imdb/
Output: s3://movies-datalake-2310/silver/imdb/
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, split, trim, when

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Silver-IMDb")

S3_BUCKET = "movies-datalake-2310"
BRONZE_IMDB = f"s3://{S3_BUCKET}/bronze/imdb"
SILVER_IMDB = f"s3://{S3_BUCKET}/silver/imdb"


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def clean_title_basics(spark: SparkSession):
    """Clean title.basics: filter movies, cast types, split genres"""
    logger.info("Processing title_basics table")

    input_path = f"{BRONZE_IMDB}/title_basics"
    logger.info(f"  Reading from: {input_path}")

    df = spark.read.parquet(input_path)
    total_count = df.count()
    logger.info(f"  Total titles: {total_count}")

    movies_df = df.filter(col("titleType") == "movie")
    movie_count = movies_df.count()
    logger.info(
        f"  Movies only: {movie_count} ({100 * movie_count / total_count:.1f}%)"
    )

    cleaned = movies_df.select(
        col("tconst").alias("imdb_id"),
        trim(col("primaryTitle")).alias("primary_title"),
        trim(col("originalTitle")).alias("original_title"),
        col("startYear").cast("int").alias("year"),
        col("runtimeMinutes").cast("int").alias("runtime_minutes"),
        col("isAdult").cast("int").alias("is_adult"),
        when(col("genres").isNull() | (col("genres") == ""), None)
        .otherwise(split(col("genres"), ","))
        .alias("genres_array"),
        col("genres").alias("genres_raw"),
    ).withColumn("cleaned_date", current_date())

    with_year = cleaned.filter(col("year").isNotNull()).count()
    with_runtime = cleaned.filter(col("runtime_minutes").isNotNull()).count()
    logger.info(f"  With year: {with_year}")
    logger.info(f"  With runtime: {with_runtime}")

    output_path = f"{SILVER_IMDB}/title_basics"
    logger.info(f"  Writing to: {output_path}")
    cleaned.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ title_basics complete ({movie_count} rows)")


def clean_title_ratings(spark: SparkSession):
    """Clean title.ratings: cast numeric types"""
    logger.info("Processing title_ratings table")

    input_path = f"{BRONZE_IMDB}/title_ratings"
    logger.info(f"  Reading from: {input_path}")

    df = spark.read.parquet(input_path)
    row_count = df.count()
    logger.info(f"  Input rows: {row_count}")

    cleaned = df.select(
        col("tconst").alias("imdb_id"),
        col("averageRating").cast("double").alias("imdb_rating"),
        col("numVotes").cast("int").alias("imdb_votes"),
    ).withColumn("cleaned_date", current_date())

    with_rating = cleaned.filter(col("imdb_rating").isNotNull()).count()
    logger.info(f"  With rating: {with_rating}")

    output_path = f"{SILVER_IMDB}/title_ratings"
    logger.info(f"  Writing to: {output_path}")
    cleaned.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ title_ratings complete ({row_count} rows)")


def clean_name_basics(spark: SparkSession):
    """Clean name.basics: cast types, extract primary profession"""
    logger.info("Processing name_basics table")

    input_path = f"{BRONZE_IMDB}/name_basics"
    logger.info(f"  Reading from: {input_path}")

    df = spark.read.parquet(input_path)
    row_count = df.count()
    logger.info(f"  Input rows: {row_count}")

    cleaned = df.select(
        col("nconst").alias("person_id"),
        trim(col("primaryName")).alias("name"),
        col("birthYear").cast("int").alias("birth_year"),
        col("deathYear").cast("int").alias("death_year"),
        split(col("primaryProfession"), ",").getItem(0).alias("primary_profession"),
        col("primaryProfession").alias("professions_raw"),
        split(col("knownForTitles"), ",").alias("known_for_titles"),
    ).withColumn("cleaned_date", current_date())

    output_path = f"{SILVER_IMDB}/name_basics"
    logger.info(f"  Writing to: {output_path}")
    cleaned.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ name_basics complete ({row_count} rows)")


def clean_title_principals(spark: SparkSession):
    """Clean title.principals: cast ordering, clean characters"""
    logger.info("Processing title_principals table")

    input_path = f"{BRONZE_IMDB}/title_principals"
    logger.info(f"  Reading from: {input_path}")

    df = spark.read.parquet(input_path)
    row_count = df.count()
    logger.info(f"  Input rows: {row_count}")

    cleaned = df.select(
        col("tconst").alias("imdb_id"),
        col("ordering").cast("int").alias("ordering"),
        col("nconst").alias("person_id"),
        trim(col("category")).alias("category"),
        trim(col("job")).alias("job"),
        trim(col("characters")).alias("characters"),
    ).withColumn("cleaned_date", current_date())

    output_path = f"{SILVER_IMDB}/title_principals"
    logger.info(f"  Writing to: {output_path}")
    cleaned.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ title_principals complete ({row_count} rows)")


def clean_title_crew(spark: SparkSession):
    """Clean title.crew: split directors and writers"""
    logger.info("Processing title_crew table")

    input_path = f"{BRONZE_IMDB}/title_crew"
    logger.info(f"  Reading from: {input_path}")

    df = spark.read.parquet(input_path)
    row_count = df.count()
    logger.info(f"  Input rows: {row_count}")

    cleaned = df.select(
        col("tconst").alias("imdb_id"),
        when(col("directors").isNull() | (col("directors") == ""), None)
        .otherwise(split(col("directors"), ","))
        .alias("directors"),
        when(col("writers").isNull() | (col("writers") == ""), None)
        .otherwise(split(col("writers"), ","))
        .alias("writers"),
    ).withColumn("cleaned_date", current_date())

    output_path = f"{SILVER_IMDB}/title_crew"
    logger.info(f"  Writing to: {output_path}")
    cleaned.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ title_crew complete ({row_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean IMDb Data")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting IMDb Silver Cleaning")
    logger.info("=" * 60)

    spark = get_spark_session("Silver-IMDb")

    try:
        clean_title_basics(spark)
        clean_title_ratings(spark)
        clean_name_basics(spark)
        clean_title_principals(spark)
        clean_title_crew(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("IMDb Silver cleaning complete")
