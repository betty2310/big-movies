"""
Silver Layer: Clean MovieLens Data

Transformations:
- Extract year from title: "Toy Story (1995)" -> 1995
- Extract clean title without year
- Split genres from pipe-separated to array
- Compute average ratings per movie

Input: s3://movies-datalake-2310/bronze/movielens/
Output: s3://movies-datalake-2310/silver/movielens/
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_date,
    regexp_extract,
    regexp_replace,
    split,
    trim,
    when,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Silver-MovieLens")

S3_BUCKET = "movies-datalake-2310"
BRONZE_MOVIELENS = f"s3://{S3_BUCKET}/bronze/movielens"
SILVER_MOVIELENS = f"s3://{S3_BUCKET}/silver/movielens"


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def clean_movies(spark: SparkSession):
    """Clean movies table: extract year, split genres"""
    logger.info("Processing movies table")

    movies_path = f"{BRONZE_MOVIELENS}/movies"
    logger.info(f"  Reading from: {movies_path}")

    df = spark.read.parquet(movies_path)
    row_count = df.count()
    logger.info(f"  Input rows: {row_count}")

    cleaned = df.select(
        col("movieId").alias("ml_id"),
        trim(regexp_replace(col("title"), r"\s*\(\d{4}\)\s*$", "")).alias("title"),
        regexp_extract(col("title"), r"\((\d{4})\)\s*$", 1).cast("int").alias("year"),
        when(col("genres") == "(no genres listed)", None)
        .otherwise(split(col("genres"), r"\|"))
        .alias("genres_array"),
        col("genres").alias("genres_raw"),
    ).withColumn("cleaned_date", current_date())

    with_year = cleaned.filter(col("year").isNotNull()).count()
    logger.info(
        f"  Movies with year extracted: {with_year} ({100 * with_year / row_count:.1f}%)"
    )

    output_path = f"{SILVER_MOVIELENS}/movies"
    logger.info(f"  Writing to: {output_path}")
    cleaned.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ movies complete ({row_count} rows)")


def aggregate_ratings(spark: SparkSession):
    """Aggregate ratings per movie: avg rating, count"""
    logger.info("Processing ratings aggregation")

    ratings_path = f"{BRONZE_MOVIELENS}/ratings"
    logger.info(f"  Reading from: {ratings_path}")

    df = spark.read.parquet(ratings_path)
    rating_count = df.count()
    logger.info(f"  Input ratings: {rating_count}")

    aggregated = (
        df.groupBy("movieId")
        .agg(
            avg("rating").alias("ml_avg_rating"),
            count("*").alias("ml_rating_count"),
        )
        .withColumnRenamed("movieId", "ml_id")
        .withColumn("aggregated_date", current_date())
    )

    movie_count = aggregated.count()
    logger.info(f"  Unique movies with ratings: {movie_count}")

    output_path = f"{SILVER_MOVIELENS}/ratings_agg"
    logger.info(f"  Writing to: {output_path}")
    aggregated.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ ratings_agg complete ({movie_count} rows)")


def clean_tags(spark: SparkSession):
    """Clean tags table"""
    logger.info("Processing tags table")

    tags_path = f"{BRONZE_MOVIELENS}/tags"
    logger.info(f"  Reading from: {tags_path}")

    df = spark.read.parquet(tags_path)
    row_count = df.count()
    logger.info(f"  Input rows: {row_count}")

    cleaned = (
        df.select(
            col("userId").alias("user_id"),
            col("movieId").alias("ml_id"),
            trim(col("tag")).alias("tag"),
            col("timestamp"),
        )
        .filter(col("tag").isNotNull() & (trim(col("tag")) != ""))
        .withColumn("cleaned_date", current_date())
    )

    valid_count = cleaned.count()
    logger.info(f"  Valid tags: {valid_count}")

    output_path = f"{SILVER_MOVIELENS}/tags"
    logger.info(f"  Writing to: {output_path}")
    cleaned.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ tags complete ({valid_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean MovieLens Data")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting MovieLens Silver Cleaning")
    logger.info("=" * 60)

    spark = get_spark_session("Silver-MovieLens")

    try:
        clean_movies(spark)
        aggregate_ratings(spark)
        clean_tags(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("MovieLens Silver cleaning complete")
