"""
Gold Layer: Build Genre Dimension

Creates canonical genre lookup table with standardized names.
Merges genres from all sources (MovieLens, IMDb, TMDB, RT) into unified list.

Output: s3://movies-datalake-2310/gold/dim_genre/
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    explode,
    lit,
    monotonically_increasing_id,
    trim,
    upper,
    when,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Gold-DimGenre")

S3_BUCKET = "movies-datalake-2310"
SILVER_PATH = f"s3://{S3_BUCKET}/silver"
GOLD_PATH = f"s3://{S3_BUCKET}/gold"

GENRE_MAPPING = {
    "SCI-FI": "Science Fiction",
    "SCIENCE FICTION": "Science Fiction",
    "SCIFI": "Science Fiction",
    "CHILDREN": "Family",
    "KIDS & FAMILY": "Family",
    "CHILDREN'S": "Family",
    "FILM-NOIR": "Film Noir",
    "FILM NOIR": "Film Noir",
    "MUSICAL": "Music",
    "WAR": "War",
    "IMAX": "IMAX",
    "(NO GENRES LISTED)": None,
}


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def normalize_genre(genre_name):
    """Normalize genre name to canonical form"""
    if genre_name is None:
        return None
    upper_name = genre_name.strip().upper()
    return GENRE_MAPPING.get(upper_name, genre_name.strip().title())


def build_dim_genre(spark: SparkSession):
    """Build genre dimension from all sources"""
    logger.info("Building genre dimension")

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    normalize_genre_udf = udf(normalize_genre, StringType())

    all_genres = []

    ml_movies_path = f"{SILVER_PATH}/movielens/movies"
    logger.info(f"  Reading MovieLens genres from: {ml_movies_path}")
    try:
        ml_df = spark.read.parquet(ml_movies_path)
        ml_genres = (
            ml_df.select(explode(col("genres_array")).alias("genre"))
            .filter(col("genre").isNotNull())
            .select(normalize_genre_udf(col("genre")).alias("genre_name"))
            .filter(col("genre_name").isNotNull())
        )
        all_genres.append(ml_genres)
        logger.info(f"    MovieLens genres extracted")
    except Exception as e:
        logger.warning(f"    Could not read MovieLens: {e}")

    imdb_path = f"{SILVER_PATH}/imdb/title_basics"
    logger.info(f"  Reading IMDb genres from: {imdb_path}")
    try:
        imdb_df = spark.read.parquet(imdb_path)
        imdb_genres = (
            imdb_df.select(explode(col("genres_array")).alias("genre"))
            .filter(col("genre").isNotNull())
            .select(normalize_genre_udf(trim(col("genre"))).alias("genre_name"))
            .filter(col("genre_name").isNotNull())
        )
        all_genres.append(imdb_genres)
        logger.info(f"    IMDb genres extracted")
    except Exception as e:
        logger.warning(f"    Could not read IMDb: {e}")

    tmdb_path = f"{SILVER_PATH}/tmdb/movies"
    logger.info(f"  Reading TMDB genres from: {tmdb_path}")
    try:
        tmdb_df = spark.read.parquet(tmdb_path)
        tmdb_genres = (
            tmdb_df.select(explode(col("genres_array")).alias("genre"))
            .filter(col("genre").isNotNull())
            .select(normalize_genre_udf(col("genre")).alias("genre_name"))
            .filter(col("genre_name").isNotNull())
        )
        all_genres.append(tmdb_genres)
        logger.info(f"    TMDB genres extracted")
    except Exception as e:
        logger.warning(f"    Could not read TMDB: {e}")

    rt_path = f"{SILVER_PATH}/rotten_tomatoes/movies"
    logger.info(f"  Reading Rotten Tomatoes genres from: {rt_path}")
    try:
        rt_df = spark.read.parquet(rt_path)
        rt_genres = (
            rt_df.select(explode(col("genres_array")).alias("genre"))
            .filter(col("genre").isNotNull())
            .select(normalize_genre_udf(trim(col("genre"))).alias("genre_name"))
            .filter(col("genre_name").isNotNull())
        )
        all_genres.append(rt_genres)
        logger.info(f"    Rotten Tomatoes genres extracted")
    except Exception as e:
        logger.warning(f"    Could not read Rotten Tomatoes: {e}")

    if not all_genres:
        raise ValueError("No genre data found from any source")

    combined = all_genres[0]
    for df in all_genres[1:]:
        combined = combined.union(df)

    unique_genres = (
        combined.select("genre_name")
        .distinct()
        .filter(col("genre_name").isNotNull())
        .filter(col("genre_name") != "")
        .orderBy("genre_name")
    )

    dim_genre = unique_genres.withColumn(
        "genre_id", (monotonically_increasing_id() + 1).cast("int")
    ).select("genre_id", "genre_name")

    dim_genre = dim_genre.withColumn("created_date", current_date())

    row_count = dim_genre.count()
    logger.info(f"  Total unique genres: {row_count}")

    dim_genre.show(50, truncate=False)

    output_path = f"{GOLD_PATH}/dim_genre"
    logger.info(f"  Writing to: {output_path}")
    dim_genre.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ dim_genre complete ({row_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Genre Dimension")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting Genre Dimension Build")
    logger.info("=" * 60)

    spark = get_spark_session("Gold-DimGenre")

    try:
        build_dim_genre(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("Genre dimension build complete")
