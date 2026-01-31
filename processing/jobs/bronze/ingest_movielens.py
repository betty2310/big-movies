"""
Bronze Layer: Ingest MovieLens CSV files to Parquet

Input: s3://movies-datalake-2310/raw/movielens/{date}/*.csv
Output: s3://movies-datalake-2310/bronze/movielens/{table}/
"""

import argparse
import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, FloatType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Bronze-MovieLens")

S3_BUCKET = "movies-datalake-2310"
RAW_MOVIELENS = f"s3://{S3_BUCKET}/raw/movielens"
BRONZE_MOVIELENS = f"s3://{S3_BUCKET}/bronze/movielens"

SCHEMAS = {
    "ratings": StructType([
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("rating", FloatType(), False),
        StructField("timestamp", LongType(), False),
    ]),
    "movies": StructType([
        StructField("movieId", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("genres", StringType(), True),
    ]),
    "tags": StructType([
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("tag", StringType(), True),
        StructField("timestamp", LongType(), False),
    ]),
    "links": StructType([
        StructField("movieId", IntegerType(), False),
        StructField("imdbId", StringType(), True),
        StructField("tmdbId", IntegerType(), True),
    ]),
}


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def get_latest_partition(spark: SparkSession, base_path: str) -> Optional[str]:
    logger.info(f"Looking for latest partition in: {base_path}")
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(base_path), hadoop_conf
        )
        path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
        if not fs.exists(path):
            logger.warning(f"Path does not exist: {base_path}")
            return None
        statuses = fs.listStatus(path)
        dirs = [
            s.getPath().getName()
            for s in statuses
            if s.isDirectory() and not s.getPath().getName().startswith("_")
        ]
        if not dirs:
            logger.warning(f"No partitions found in: {base_path}")
            return None
        dirs.sort(reverse=True)
        latest = f"{base_path}/{dirs[0]}"
        logger.info(f"Latest partition: {latest}")
        return latest
    except Exception as e:
        logger.error(f"Error finding latest partition: {e}")
        return None


def ingest_movielens(input_date: Optional[str] = None):
    spark = get_spark_session("Bronze-MovieLens")

    if input_date:
        input_path = f"{RAW_MOVIELENS}/{input_date}"
    else:
        input_path = get_latest_partition(spark, RAW_MOVIELENS)
        if not input_path:
            raise ValueError(f"No data found in {RAW_MOVIELENS}")

    logger.info(f"Processing MovieLens data from: {input_path}")

    for table_name, schema in SCHEMAS.items():
        csv_path = f"{input_path}/{table_name}.csv"
        logger.info(f"Reading: {csv_path}")

        try:
            df = spark.read.csv(csv_path, schema=schema, header=True)
            df = df.withColumn("ingest_date", current_date())

            row_count = df.count()
            logger.info(f"  Read {row_count} rows from {table_name}")

            output_path = f"{BRONZE_MOVIELENS}/{table_name}"
            logger.info(f"  Writing to: {output_path}")

            df.write.mode("overwrite").parquet(output_path)
            logger.info(f"  ✓ {table_name} complete")

        except Exception as e:
            logger.error(f"  ✗ Failed to process {table_name}: {e}")
            raise

    spark.stop()
    logger.info("MovieLens Bronze ingestion complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest MovieLens CSV to Bronze Parquet")
    parser.add_argument("--input-date", type=str, default=None, help="Date partition (YYYY-MM-DD)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting MovieLens Bronze Ingestion")
    logger.info("=" * 60)

    try:
        ingest_movielens(args.input_date)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
