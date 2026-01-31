"""
Gold Layer: Load to PostgreSQL

Loads all gold layer tables to PostgreSQL data warehouse via JDBC.
Uses truncate + insert (full refresh) strategy.

Input: s3://movies-datalake-2310/gold/
Output: PostgreSQL tables

Environment Variables:
- POSTGRES_HOST: Database host
- POSTGRES_PORT: Database port (default: 5432)
- POSTGRES_DB: Database name
- POSTGRES_USER: Username
- POSTGRES_PASSWORD: Password
"""

import argparse
import logging
import os

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Gold-LoadPostgres")

S3_BUCKET = "movies-datalake-2310"
GOLD_PATH = f"s3://{S3_BUCKET}/gold"

TABLES = [
    "dim_time",
    "dim_genre",
    "dim_person",
    "dim_movie",
    "fact_movie_metrics",
    "bridge_movie_cast",
    "bridge_movie_genres",
]


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


def get_jdbc_properties():
    """Get JDBC connection properties from environment"""
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DB", "movies_dw")
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
        "rewriteBatchedStatements": "true",
    }

    return jdbc_url, properties


def load_table(spark: SparkSession, table_name: str, jdbc_url: str, properties: dict):
    """Load a single table from S3 to PostgreSQL"""
    logger.info(f"  Loading {table_name}...")

    input_path = f"{GOLD_PATH}/{table_name}"

    try:
        df = spark.read.parquet(input_path)
        row_count = df.count()
        logger.info(f"    Read {row_count} rows from S3")

        if "created_date" in df.columns:
            df = df.drop("created_date")

        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=properties,
        )

        logger.info(f"    ✓ {table_name} loaded ({row_count} rows)")
        return row_count

    except Exception as e:
        logger.error(f"    ✗ Failed to load {table_name}: {e}")
        raise


def load_postgres(spark: SparkSession):
    """Load all gold tables to PostgreSQL"""
    logger.info("Loading gold layer to PostgreSQL")

    jdbc_url, properties = get_jdbc_properties()
    logger.info(f"  JDBC URL: {jdbc_url}")
    logger.info(f"  User: {properties['user']}")

    total_rows = 0
    loaded_tables = 0

    for table_name in TABLES:
        try:
            rows = load_table(spark, table_name, jdbc_url, properties)
            total_rows += rows
            loaded_tables += 1
        except Exception as e:
            logger.warning(f"  Skipping {table_name} due to error: {e}")

    logger.info("=" * 60)
    logger.info("PostgreSQL Load Summary")
    logger.info("=" * 60)
    logger.info(f"  Tables loaded: {loaded_tables}/{len(TABLES)}")
    logger.info(f"  Total rows: {total_rows:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Gold Layer to PostgreSQL")
    parser.add_argument("--tables", type=str, default=None, help="Comma-separated list of tables to load")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting PostgreSQL Load")
    logger.info("=" * 60)

    if args.tables:
        TABLES = [t.strip() for t in args.tables.split(",")]
        logger.info(f"Loading specific tables: {TABLES}")

    spark = get_spark_session("Gold-LoadPostgres")

    try:
        load_postgres(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("PostgreSQL load complete")
