"""
Gold Layer PostgreSQL Loader

Loads Parquet files from S3 gold layer to Supabase PostgreSQL.
Uses truncate + insert (full refresh) strategy with batch inserts.
"""

import io
import logging
import os
from typing import Optional

import boto3
import psycopg2
import pyarrow.parquet as pq
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("PostgresLoader")

S3_BUCKET = os.environ.get("S3_BUCKET", "movies-datalake-2310")
GOLD_PATH = "gold"

TABLES_LOAD_ORDER = [
    "dim_time",
    "dim_genre",
    "dim_person",
    "dim_movie",
    "fact_movie_metrics",
    "bridge_movie_genres",
    "bridge_movie_cast",
]

BATCH_SIZE = 10000


def get_connection():
    """Get PostgreSQL connection from DATABASE_URL or individual env vars."""
    database_url = os.environ.get("DATABASE_URL")

    if database_url:
        return psycopg2.connect(database_url)

    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def get_s3_client():
    """Get boto3 S3 client."""
    return boto3.client("s3")


def list_parquet_files(s3_client, table_name: str) -> list[str]:
    """List all parquet files for a table in S3."""
    prefix = f"{GOLD_PATH}/{table_name}/"
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

    files = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".parquet"):
            files.append(key)

    return files


def read_parquet_from_s3(s3_client, key: str) -> pq.ParquetFile:
    """Read a parquet file from S3 into a PyArrow table."""
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    data = response["Body"].read()
    return pq.ParquetFile(io.BytesIO(data))


def get_table_columns(
    parquet_file: pq.ParquetFile,
    exclude: Optional[list[str]] = None,
) -> list[str]:
    """Get column names from parquet schema, excluding specified columns."""
    exclude = exclude or []
    return [col for col in parquet_file.schema_arrow.names if col not in exclude]


def convert_value(x):
    """Convert a single value to PostgreSQL-compatible type."""
    import numpy as np

    # Handle None and NaN
    if x is None:
        return None

    # Handle pandas NA first (before type checks)
    try:
        import pandas as pd

        if pd.isna(x):
            return None
    except (ImportError, TypeError, ValueError):
        pass

    # Handle NaN and infinity for floats
    if isinstance(x, float) and (x != x or np.isinf(x)):
        return None

    # Handle Python native float - convert whole numbers to int
    if isinstance(x, float):
        if x.is_integer():
            val = int(x)
            # Ensure value fits in PostgreSQL INTEGER range
            if val < -2147483648 or val > 2147483647:
                return None
            return val
        return x

    # Handle numpy arrays -> comma-separated string
    if isinstance(x, np.ndarray):
        return ",".join(str(v) for v in x.tolist())

    # Handle lists/tuples -> comma-separated string
    if isinstance(x, (list, tuple)):
        return ",".join(str(v) for v in x)

    # Handle numpy scalar types -> Python native
    if isinstance(x, np.integer):
        val = int(x)
        # Ensure value fits in PostgreSQL INTEGER range
        if val < -2147483648 or val > 2147483647:
            return None
        return val
    if isinstance(x, np.floating):
        val = float(x)
        if val != val or np.isinf(val):
            return None
        # Convert whole numbers to int for INTEGER columns
        if val.is_integer():
            intval = int(val)
            # Ensure value fits in PostgreSQL INTEGER range
            if intval < -2147483648 or intval > 2147483647:
                return None
            return intval
        return val
    if isinstance(x, np.bool_):
        return bool(x)

    return x


def convert_dataframe(df):
    """Convert all DataFrame values to PostgreSQL-compatible types."""
    for c in df.columns:
        df[c] = df[c].apply(convert_value)
    return df


def load_table(
    conn,
    s3_client,
    table_name: str,
    truncate: bool = True,
    exclude_columns: Optional[list[str]] = None,
) -> int:
    """
    Load a single table from S3 parquet to PostgreSQL.

    Args:
        conn: PostgreSQL connection
        s3_client: boto3 S3 client
        table_name: Name of the table to load
        truncate: Whether to truncate before insert
        exclude_columns: Columns to exclude (e.g., created_date)

    Returns:
        Number of rows loaded
    """
    exclude_columns = exclude_columns or ["created_date"]
    logger.info(f"Loading {table_name}...")

    parquet_files = list_parquet_files(s3_client, table_name)
    if not parquet_files:
        logger.warning(f"  No parquet files found for {table_name}")
        return 0

    logger.info(f"  Found {len(parquet_files)} parquet file(s)")

    cursor = conn.cursor()

    if truncate:
        logger.info(f"  Truncating {table_name}...")
        cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
        conn.commit()

    total_rows = 0
    skipped_rows = 0
    columns = None

    for parquet_key in parquet_files:
        logger.info(f"  Processing {parquet_key}...")
        parquet_file = read_parquet_from_s3(s3_client, parquet_key)

        if columns is None:
            columns = get_table_columns(parquet_file, exclude_columns)
            columns_str = ", ".join(columns)
            logger.info(f"  Columns: {columns_str}")

        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
            df = batch.to_pandas()
            df = df[[col for col in columns if col in df.columns]]

            # Convert at tuple extraction to avoid pandas dtype reconversion
            rows = [
                tuple(convert_value(v) for v in row)
                for row in df.itertuples(index=False, name=None)
            ]

            if rows:
                try:
                    execute_values(
                        cursor,
                        f"INSERT INTO {table_name} ({columns_str}) VALUES %s",
                        rows,
                        page_size=BATCH_SIZE,
                    )
                    conn.commit()
                    total_rows += len(rows)

                    if total_rows % 10000 == 0:
                        logger.info(f"    Loaded {total_rows:,} rows...")
                except Exception as e:
                    # If batch insert fails, try row by row to skip bad rows
                    conn.rollback()
                    logger.warning(
                        f"    Batch insert failed: {e}. Trying row-by-row..."
                    )

                    for row in rows:
                        try:
                            execute_values(
                                cursor,
                                f"INSERT INTO {table_name} ({columns_str}) VALUES %s",
                                [row],
                                page_size=1,
                            )
                            conn.commit()
                            total_rows += 1
                        except Exception as row_error:
                            logger.warning(row)
                            skipped_rows += 1
                            logger.warning(
                                f"    Skipping row due to error: {row_error}"
                            )
                            conn.rollback()

    if skipped_rows > 0:
        logger.warning(f"  ⚠ Skipped {skipped_rows:,} rows due to data errors")

    logger.info(f"  ✓ {table_name} complete ({total_rows:,} rows)")
    cursor.close()
    return total_rows


def load_all_tables(
    tables: Optional[list[str]] = None,
    truncate: bool = True,
) -> dict[str, dict]:
    """
    Load all gold layer tables to PostgreSQL.

    Args:
        tables: List of tables to load (default: all tables in order)
        truncate: Whether to truncate before insert

    Returns:
        Dictionary of table_name -> {'rows': count, 'skipped': count}
    """
    tables = tables or TABLES_LOAD_ORDER

    logger.info("=" * 60)
    logger.info("Starting PostgreSQL Load")
    logger.info("=" * 60)
    logger.info(f"  S3 Bucket: {S3_BUCKET}")
    logger.info(f"  Tables: {', '.join(tables)}")

    conn = get_connection()
    s3_client = get_s3_client()

    results = {}

    try:
        for table_name in tables:
            if table_name not in TABLES_LOAD_ORDER:
                logger.warning(f"  Unknown table: {table_name}, skipping")
                continue

            try:
                row_count = load_table(conn, s3_client, table_name, truncate=truncate)
                results[table_name] = row_count
            except Exception as e:
                logger.error(f"  ✗ Failed to load {table_name}: {e}")
                results[table_name] = -1

    finally:
        conn.close()

    logger.info("=" * 60)
    logger.info("PostgreSQL Load Summary")
    logger.info("=" * 60)

    total_rows = 0
    success_count = 0
    for table, count in results.items():
        if count >= 0:
            logger.info(f"  {table}: {count:,} rows loaded")
            total_rows += count
            success_count += 1
        else:
            logger.info(f"  {table}: FAILED")

    logger.info("-" * 60)
    logger.info(f"  Tables loaded: {success_count}/{len(tables)}")
    logger.info(f"  Total rows: {total_rows:,}")

    return results


def validate_load(tables: Optional[list[str]] = None) -> dict[str, dict]:
    """
    Validate loaded data by checking row counts and basic integrity.

    Returns:
        Dictionary with validation results per table
    """
    tables = tables or TABLES_LOAD_ORDER

    logger.info("=" * 60)
    logger.info("Validating PostgreSQL Load")
    logger.info("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    results = {}

    try:
        for table_name in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            results[table_name] = {"row_count": count}
            logger.info(f"  {table_name}: {count:,} rows")

        cursor.execute("""
            SELECT COUNT(*) FROM fact_movie_metrics f
            WHERE NOT EXISTS (SELECT 1 FROM dim_movie d WHERE d.movie_id = f.movie_id)
        """)
        orphan_facts = cursor.fetchone()[0]
        results["_orphan_facts"] = orphan_facts
        if orphan_facts > 0:
            logger.warning(f"  ⚠ Orphan fact records: {orphan_facts}")
        else:
            logger.info("  ✓ Referential integrity: OK")

    finally:
        cursor.close()
        conn.close()

    return results
