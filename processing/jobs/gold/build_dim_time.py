"""
Gold Layer: Build Time Dimension

Generates a complete time dimension table from 1950 to 2030.
Pre-computed calendar attributes for efficient analytics.

Output: s3://movies-datalake-2310/gold/dim_time/
"""

import argparse
import logging
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    dayofmonth,
    dayofweek,
    dayofyear,
    month,
    quarter,
    weekofyear,
    year,
)
from pyspark.sql.types import DateType, IntegerType, StructField, StructType

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Gold-DimTime")

S3_BUCKET = "movies-datalake-2310"
GOLD_PATH = f"s3://{S3_BUCKET}/gold"


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def build_dim_time(spark: SparkSession):
    """Build time dimension from 1950 to 2030"""
    logger.info("Building time dimension")

    start_date = date(1950, 1, 1)
    end_date = date(2030, 12, 31)

    dates = []
    current = start_date
    while current <= end_date:
        dates.append((current,))
        current += timedelta(days=1)

    logger.info(f"  Generating {len(dates)} date records")

    schema = StructType([StructField("date", DateType(), False)])
    df = spark.createDataFrame(dates, schema)

    dim_time = df.select(
        (year(col("date")) * 10000 + month(col("date")) * 100 + dayofmonth(col("date")))
        .cast(IntegerType())
        .alias("time_id"),
        col("date"),
        year(col("date")).alias("year"),
        quarter(col("date")).alias("quarter"),
        month(col("date")).alias("month"),
        dayofmonth(col("date")).alias("day"),
        dayofweek(col("date")).alias("day_of_week"),
        dayofyear(col("date")).alias("day_of_year"),
        weekofyear(col("date")).alias("week_of_year"),
    ).withColumn("created_date", current_date())

    row_count = dim_time.count()
    logger.info(f"  Total records: {row_count}")

    output_path = f"{GOLD_PATH}/dim_time"
    logger.info(f"  Writing to: {output_path}")
    dim_time.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ dim_time complete ({row_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Time Dimension")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting Time Dimension Build")
    logger.info("=" * 60)

    spark = get_spark_session("Gold-DimTime")

    try:
        build_dim_time(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("Time dimension build complete")
