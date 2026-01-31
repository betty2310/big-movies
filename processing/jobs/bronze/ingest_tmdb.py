"""
Bronze Layer: Ingest TMDB NDJSON files to Parquet

Input: s3://movies-datalake-2310/raw/tmdb/{date}/*.ndjson
Output: s3://movies-datalake-2310/bronze/tmdb/movies/
"""

import argparse
import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Bronze-TMDB")

S3_BUCKET = "movies-datalake-2310"
RAW_TMDB = f"s3://{S3_BUCKET}/raw/tmdb"
BRONZE_TMDB = f"s3://{S3_BUCKET}/bronze/tmdb"

TMDB_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("poster_path", StringType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("genre_ids", ArrayType(IntegerType()), True),
    StructField("original_language", StringType(), True),
    StructField("adult", BooleanType(), True),
    StructField("video", BooleanType(), True),
])


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


def ingest_tmdb(input_date: Optional[str] = None):
    spark = get_spark_session("Bronze-TMDB")

    if input_date:
        input_path = f"{RAW_TMDB}/{input_date}"
    else:
        input_path = get_latest_partition(spark, RAW_TMDB)
        if not input_path:
            raise ValueError(f"No data found in {RAW_TMDB}")

    logger.info(f"Processing TMDB data from: {input_path}")

    ndjson_path = f"{input_path}/movies_*.ndjson"
    logger.info(f"Reading: {ndjson_path}")

    try:
        df = spark.read.schema(TMDB_SCHEMA).json(ndjson_path)
        logger.info(f"  Raw rows: {df.count()}")

        df = df.dropDuplicates(["id"])
        df = df.withColumn("ingest_date", current_date())

        row_count = df.count()
        logger.info(f"  After dedup: {row_count} rows")

        output_path = f"{BRONZE_TMDB}/movies"
        logger.info(f"  Writing to: {output_path}")

        df.write.mode("overwrite").parquet(output_path)
        logger.info(f"  ✓ movies complete")

    except Exception as e:
        logger.error(f"  ✗ Failed to process TMDB: {e}")
        raise

    spark.stop()
    logger.info("TMDB Bronze ingestion complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest TMDB NDJSON to Bronze Parquet")
    parser.add_argument("--input-date", type=str, default=None, help="Date partition (YYYY-MM-DD)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting TMDB Bronze Ingestion")
    logger.info("=" * 60)

    try:
        ingest_tmdb(args.input_date)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
