"""
Gold Layer: Build Bridge Movie Cast

Creates many-to-many relationship between movies and persons (actors, directors, etc.)
Links dim_movie to dim_person with role information.

Input:
- s3://movies-datalake-2310/silver/entity_spine/
- s3://movies-datalake-2310/silver/imdb/title_principals/

Output: s3://movies-datalake-2310/gold/bridge_movie_cast/
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Gold-BridgeCast")

S3_BUCKET = "movies-datalake-2310"
SILVER_PATH = f"s3://{S3_BUCKET}/silver"
GOLD_PATH = f"s3://{S3_BUCKET}/gold"


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def build_bridge_cast(spark: SparkSession):
    """Build bridge table between movies and cast/crew"""
    logger.info("Building bridge_movie_cast")

    spine_path = f"{SILVER_PATH}/entity_spine"
    logger.info(f"  Reading entity spine from: {spine_path}")
    spine_df = spark.read.parquet(spine_path).select("movie_id", "imdb_id")
    spine_count = spine_df.count()
    logger.info(f"    Entity spine records: {spine_count}")

    principals_path = f"{SILVER_PATH}/imdb/title_principals"
    logger.info(f"  Reading IMDb principals from: {principals_path}")
    principals_df = spark.read.parquet(principals_path).select(
        col("imdb_id"),
        col("person_id"),
        col("ordering"),
        col("category"),
        col("job"),
        col("characters"),
    )
    principals_count = principals_df.count()
    logger.info(f"    IMDb principals records: {principals_count}")

    # Read dim_person to filter only valid persons (those with birth_year)
    dim_person_path = f"{GOLD_PATH}/dim_person"
    logger.info(f"  Reading dim_person from: {dim_person_path}")
    dim_person_df = spark.read.parquet(dim_person_path).select("person_id")
    person_count = dim_person_df.count()
    logger.info(f"    Valid persons: {person_count}")

    logger.info("  Joining spine with principals...")
    bridge_cast = spine_df.join(principals_df, on="imdb_id", how="inner")
    
    # Filter to only persons that exist in dim_person
    logger.info("  Filtering to valid persons only...")
    bridge_cast = bridge_cast.join(dim_person_df, on="person_id", how="inner")

    bridge_cast = bridge_cast.select(
        col("movie_id"),
        col("person_id"),
        col("category"),
        col("ordering"),
        col("job"),
        col("characters"),
    ).withColumn("created_date", current_date())

    bridge_cast = bridge_cast.dropDuplicates(["movie_id", "person_id", "ordering"])

    final_count = bridge_cast.count()
    unique_movies = bridge_cast.select("movie_id").distinct().count()
    unique_persons = bridge_cast.select("person_id").distinct().count()

    category_counts = bridge_cast.groupBy("category").count().orderBy(col("count").desc())

    logger.info("=" * 60)
    logger.info("Bridge Movie Cast Statistics")
    logger.info("=" * 60)
    logger.info(f"  Total records: {final_count}")
    logger.info(f"  Unique movies: {unique_movies}")
    logger.info(f"  Unique persons: {unique_persons}")
    logger.info("  By category:")
    for row in category_counts.collect()[:10]:
        logger.info(f"    {row['category']}: {row['count']}")

    output_path = f"{GOLD_PATH}/bridge_movie_cast"
    logger.info(f"  Writing to: {output_path}")
    bridge_cast.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ bridge_movie_cast complete ({final_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Bridge Movie Cast")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting Bridge Movie Cast Build")
    logger.info("=" * 60)

    spark = get_spark_session("Gold-BridgeCast")

    try:
        build_bridge_cast(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("Bridge movie cast build complete")
