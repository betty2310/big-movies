"""
Bronze Layer: Ingest IMDb TSV files to Parquet

Input: s3://movies-datalake-2310/raw/imdb/{date}/*.tsv
Output: s3://movies-datalake-2310/bronze/imdb/{table}/

Key transformation: Replace \\N with null values
"""

import argparse
import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Bronze-IMDb")

S3_BUCKET = "movies-datalake-2310"
RAW_IMDB = f"s3://{S3_BUCKET}/raw/imdb"
BRONZE_IMDB = f"s3://{S3_BUCKET}/bronze/imdb"

SCHEMAS = {
    "title.basics": StructType([
        StructField("tconst", StringType(), False),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", StringType(), True),
        StructField("startYear", StringType(), True),
        StructField("endYear", StringType(), True),
        StructField("runtimeMinutes", StringType(), True),
        StructField("genres", StringType(), True),
    ]),
    "title.ratings": StructType([
        StructField("tconst", StringType(), False),
        StructField("averageRating", StringType(), True),
        StructField("numVotes", StringType(), True),
    ]),
    "name.basics": StructType([
        StructField("nconst", StringType(), False),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", StringType(), True),
        StructField("deathYear", StringType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True),
    ]),
    "title.principals": StructType([
        StructField("tconst", StringType(), False),
        StructField("ordering", StringType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True),
    ]),
    "title.crew": StructType([
        StructField("tconst", StringType(), False),
        StructField("directors", StringType(), True),
        StructField("writers", StringType(), True),
    ]),
    "title.akas": StructType([
        StructField("titleId", StringType(), False),
        StructField("ordering", StringType(), True),
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("isOriginalTitle", StringType(), True),
    ]),
    "title.episode": StructType([
        StructField("tconst", StringType(), False),
        StructField("parentTconst", StringType(), True),
        StructField("seasonNumber", StringType(), True),
        StructField("episodeNumber", StringType(), True),
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


def ingest_imdb(input_date: Optional[str] = None):
    spark = get_spark_session("Bronze-IMDb")

    if input_date:
        input_path = f"{RAW_IMDB}/{input_date}"
    else:
        input_path = get_latest_partition(spark, RAW_IMDB)
        if not input_path:
            raise ValueError(f"No data found in {RAW_IMDB}")

    logger.info(f"Processing IMDb data from: {input_path}")

    for table_name, schema in SCHEMAS.items():
        tsv_path = f"{input_path}/{table_name}.tsv"
        output_name = table_name.replace(".", "_")
        logger.info(f"Reading: {tsv_path}")

        try:
            df = spark.read.csv(
                tsv_path,
                schema=schema,
                header=True,
                sep="\t",
                nullValue="\\N",
            )
            df = df.withColumn("ingest_date", current_date())

            row_count = df.count()
            logger.info(f"  Read {row_count} rows from {table_name}")

            output_path = f"{BRONZE_IMDB}/{output_name}"
            logger.info(f"  Writing to: {output_path}")

            df.write.mode("overwrite").parquet(output_path)
            logger.info(f"  ✓ {output_name} complete")

        except Exception as e:
            logger.error(f"  ✗ Failed to process {table_name}: {e}")
            raise

    spark.stop()
    logger.info("IMDb Bronze ingestion complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest IMDb TSV to Bronze Parquet")
    parser.add_argument("--input-date", type=str, default=None, help="Date partition (YYYY-MM-DD)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting IMDb Bronze Ingestion")
    logger.info("=" * 60)

    try:
        ingest_imdb(args.input_date)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
