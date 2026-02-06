"""
Bronze Layer: Ingest Rotten Tomatoes NDJSON files to Parquet

Input: s3://movies-datalake-2310/raw/rotten_tomatoes/{date}/*.ndjson
Output: s3://movies-datalake-2310/bronze/rotten_tomatoes/movies/
"""

import argparse
import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Bronze-RottenTomatoes")

S3_BUCKET = "movies-datalake-2310"
RAW_RT = f"s3://{S3_BUCKET}/raw/rotten_tomatoes"
BRONZE_RT = f"s3://{S3_BUCKET}/bronze/rotten_tomatoes"

RT_SCHEMA = StructType(
    [
        StructField("rt_id", StringType(), False),
        StructField("slug", StringType(), True),
        StructField("title", StringType(), True),
        StructField("tomatometer_score", IntegerType(), True),
        StructField("audience_score", IntegerType(), True),
        StructField("mpaa_rating", StringType(), True),
        StructField("box_office", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("release_year", IntegerType(), True),
        StructField("director", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("scraped_at", StringType(), True),
    ]
)


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
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


def ingest_rotten_tomatoes(input_date: Optional[str] = None):
    spark = get_spark_session("Bronze-RottenTomatoes")

    if input_date:
        input_path = f"{RAW_RT}/{input_date}"
    else:
        input_path = get_latest_partition(spark, RAW_RT)
        if not input_path:
            raise ValueError(f"No data found in {RAW_RT}")

    logger.info(f"Processing Rotten Tomatoes data from: {input_path}")

    ndjson_path = f"{input_path}/movies_*.ndjson"
    logger.info(f"Reading: {ndjson_path}")

    try:
        df = spark.read.schema(RT_SCHEMA).json(ndjson_path)
        logger.info(f"  Raw rows: {df.count()}")

        df = df.dropDuplicates(["rt_id"])
        df = df.withColumn("ingest_date", current_date())

        row_count = df.count()
        logger.info(f"  After dedup: {row_count} rows")

        output_path = f"{BRONZE_RT}/movies"
        logger.info(f"  Writing to: {output_path}")

        df.write.mode("overwrite").parquet(output_path)
        logger.info("  ✓ movies complete")

    except Exception as e:
        logger.error(f"  ✗ Failed to process Rotten Tomatoes: {e}")
        raise

    spark.stop()
    logger.info("Rotten Tomatoes Bronze ingestion complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest Rotten Tomatoes NDJSON to Bronze Parquet"
    )
    parser.add_argument(
        "--input-date", type=str, default=None, help="Date partition (YYYY-MM-DD)"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting Rotten Tomatoes Bronze Ingestion")
    logger.info("=" * 60)

    try:
        ingest_rotten_tomatoes(args.input_date)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
