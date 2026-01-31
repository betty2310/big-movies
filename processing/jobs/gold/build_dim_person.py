"""
Gold Layer: Build Person Dimension

Creates person dimension from IMDb name.basics data.
Includes actors, directors, writers, and other crew.

Input: s3://movies-datalake-2310/silver/imdb/name_basics/
Output: s3://movies-datalake-2310/gold/dim_person/
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Gold-DimPerson")

S3_BUCKET = "movies-datalake-2310"
SILVER_IMDB = f"s3://{S3_BUCKET}/silver/imdb"
GOLD_PATH = f"s3://{S3_BUCKET}/gold"


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def build_dim_person(spark: SparkSession):
    """Build person dimension from IMDb name.basics"""
    logger.info("Building person dimension")

    input_path = f"{SILVER_IMDB}/name_basics"
    logger.info(f"  Reading from: {input_path}")

    df = spark.read.parquet(input_path)
    total_count = df.count()
    logger.info(f"  Total persons: {total_count}")

    dim_person = df.select(
        col("person_id"),
        col("name"),
        col("birth_year"),
        col("death_year"),
        col("primary_profession"),
        col("professions_raw").alias("all_professions"),
        col("known_for_titles"),
    ).withColumn("created_date", current_date())

    with_birth = dim_person.filter(col("birth_year").isNotNull()).count()
    with_death = dim_person.filter(col("death_year").isNotNull()).count()
    with_profession = dim_person.filter(col("primary_profession").isNotNull()).count()

    logger.info(f"  With birth year: {with_birth} ({100 * with_birth / total_count:.1f}%)")
    logger.info(f"  With death year: {with_death} ({100 * with_death / total_count:.1f}%)")
    logger.info(f"  With profession: {with_profession} ({100 * with_profession / total_count:.1f}%)")

    dim_person = dim_person.dropDuplicates(["person_id"])
    final_count = dim_person.count()

    output_path = f"{GOLD_PATH}/dim_person"
    logger.info(f"  Writing to: {output_path}")
    dim_person.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ dim_person complete ({final_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Person Dimension")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting Person Dimension Build")
    logger.info("=" * 60)

    spark = get_spark_session("Gold-DimPerson")

    try:
        build_dim_person(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("Person dimension build complete")
