"""
Silver Layer: Clean Rotten Tomatoes Data

Transformations:
- Parse box office strings: $100M -> 100000000, $1.2B -> 1200000000
- Normalize titles for matching
- Split directors from comma-separated to array
- Split genres from comma-separated to array
- Calculate divisive score (|tomatometer - audience|)

Input: s3://movies-datalake-2310/bronze/rotten_tomatoes/movies/
Output: s3://movies-datalake-2310/silver/rotten_tomatoes/movies/
"""

import argparse
import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (
    col,
    current_date,
    lower,
    regexp_replace,
    split,
    trim,
    udf,
    when,
)
from pyspark.sql.types import LongType

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Silver-RottenTomatoes")

S3_BUCKET = "movies-datalake-2310"
BRONZE_RT = f"s3://{S3_BUCKET}/bronze/rotten_tomatoes"
SILVER_RT = f"s3://{S3_BUCKET}/silver/rotten_tomatoes"


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def parse_box_office_py(value: Optional[str]) -> Optional[int]:
    """Parse box office strings to integer cents.

    Handles formats:
    - $100M -> 100_000_000
    - $1.2B -> 1_200_000_000
    - $950K -> 950_000
    - $12,345,678 -> 12_345_678
    """
    if value is None or value.strip() == "":
        return None

    try:
        value = value.upper().replace("$", "").replace(",", "").strip()

        multipliers = {
            "K": 1_000,
            "M": 1_000_000,
            "B": 1_000_000_000,
        }

        for suffix, mult in multipliers.items():
            if value.endswith(suffix):
                num_str = value[:-1]
                return int(float(num_str) * mult)

        return int(float(value))
    except (ValueError, TypeError):
        return None


def clean_rt_movies(spark: SparkSession):
    """Clean Rotten Tomatoes movies"""
    logger.info("Processing Rotten Tomatoes movies")

    input_path = f"{BRONZE_RT}/movies"
    logger.info(f"  Reading from: {input_path}")

    df = spark.read.parquet(input_path)
    row_count = df.count()
    logger.info(f"  Input rows: {row_count}")

    parse_box_office_udf = udf(parse_box_office_py, LongType())

    normalized_title_expr = trim(lower(col("title")))
    normalized_title_expr = regexp_replace(normalized_title_expr, r"[^\w\s]", "")
    normalized_title_expr = regexp_replace(normalized_title_expr, r"\s+", " ")

    cleaned = df.select(
        col("rt_id"),
        col("slug").alias("rt_slug"),
        col("title"),
        normalized_title_expr.alias("normalized_title"),
        col("tomatometer_score"),
        col("audience_score"),
        when(
            col("tomatometer_score").isNotNull() & col("audience_score").isNotNull(),
            spark_abs(col("tomatometer_score") - col("audience_score")),
        )
        .otherwise(None)
        .alias("divisive_score"),
        col("mpaa_rating"),
        col("box_office").alias("box_office_raw"),
        parse_box_office_udf(col("box_office")).alias("box_office_revenue"),
        col("release_date"),
        col("release_year"),
        when(
            col("director").isNotNull() & (col("director") != ""),
            split(trim(col("director")), r",\s*"),
        )
        .otherwise(None)
        .alias("directors"),
        when(
            col("genre").isNotNull() & (col("genre") != ""),
            split(trim(col("genre")), r",\s*"),
        )
        .otherwise(None)
        .alias("genres_array"),
        col("genre").alias("genre_raw"),
    ).withColumn("cleaned_date", current_date())

    with_tomatometer = cleaned.filter(col("tomatometer_score").isNotNull()).count()
    with_audience = cleaned.filter(col("audience_score").isNotNull()).count()
    with_both_scores = cleaned.filter(
        col("tomatometer_score").isNotNull() & col("audience_score").isNotNull()
    ).count()
    with_box_office = cleaned.filter(col("box_office_revenue").isNotNull()).count()
    box_office_raw_count = df.filter(col("box_office").isNotNull()).count()

    logger.info(
        f"  With tomatometer: {with_tomatometer} ({100 * with_tomatometer / row_count:.1f}%)"
    )
    logger.info(
        f"  With audience score: {with_audience} ({100 * with_audience / row_count:.1f}%)"
    )
    logger.info(f"  With both scores (divisive_score): {with_both_scores}")
    logger.info(
        f"  Box office parsed: {with_box_office} of {box_office_raw_count} raw values"
    )

    output_path = f"{SILVER_RT}/movies"
    logger.info(f"  Writing to: {output_path}")
    cleaned.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ movies complete ({row_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean Rotten Tomatoes Data")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting Rotten Tomatoes Silver Cleaning")
    logger.info("=" * 60)

    spark = get_spark_session("Silver-RottenTomatoes")

    try:
        clean_rt_movies(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("Rotten Tomatoes Silver cleaning complete")
