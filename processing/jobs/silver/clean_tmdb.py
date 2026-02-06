"""
Silver Layer: Clean TMDB Data

Transformations:
- Build full poster URLs from relative paths
- Map genre_ids to genre names
- Extract year from release_date

Input: s3://movies-datalake-2310/bronze/tmdb/movies/
Output: s3://movies-datalake-2310/silver/tmdb/movies/
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    current_date,
    lit,
    substring,
    transform,
    when,
)
from pyspark.sql.types import IntegerType, MapType, StringType

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Silver-TMDB")

S3_BUCKET = "movies-datalake-2310"
BRONZE_TMDB = f"s3://{S3_BUCKET}/bronze/tmdb"
SILVER_TMDB = f"s3://{S3_BUCKET}/silver/tmdb"

TMDB_POSTER_BASE = "https://image.tmdb.org/t/p/w500"

TMDB_GENRE_MAP = {
    28: "Action",
    12: "Adventure",
    16: "Animation",
    35: "Comedy",
    80: "Crime",
    99: "Documentary",
    18: "Drama",
    10751: "Family",
    14: "Fantasy",
    36: "History",
    27: "Horror",
    10402: "Music",
    9648: "Mystery",
    10749: "Romance",
    878: "Science Fiction",
    10770: "TV Movie",
    53: "Thriller",
    10752: "War",
    37: "Western",
}


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def clean_tmdb_movies(spark: SparkSession):
    """Clean TMDB movies: poster URLs, genre names, year extraction"""
    logger.info("Processing TMDB movies")

    input_path = f"{BRONZE_TMDB}/movies"
    logger.info(f"  Reading from: {input_path}")

    df = spark.read.parquet(input_path)
    row_count = df.count()
    logger.info(f"  Input rows: {row_count}")

    genre_map_broadcast = spark.sparkContext.broadcast(TMDB_GENRE_MAP)

    def map_genre_ids(genre_ids):
        if genre_ids is None:
            return None
        gmap = genre_map_broadcast.value
        return [gmap.get(gid, f"Unknown({gid})") for gid in genre_ids]

    from pyspark.sql.functions import udf
    from pyspark.sql.types import ArrayType, StringType

    map_genre_udf = udf(map_genre_ids, ArrayType(StringType()))

    cleaned = df.select(
        col("id").alias("tmdb_id"),
        col("title"),
        col("original_title"),
        col("overview").alias("plot_summary"),
        when(
            col("poster_path").isNotNull(),
            concat(lit(TMDB_POSTER_BASE), col("poster_path")),
        )
        .otherwise(None)
        .alias("poster_url"),
        when(
            col("backdrop_path").isNotNull(),
            concat(lit("https://image.tmdb.org/t/p/original"), col("backdrop_path")),
        )
        .otherwise(None)
        .alias("backdrop_url"),
        col("release_date"),
        when(
            col("release_date").isNotNull() & (col("release_date") != ""),
            substring(col("release_date"), 1, 4).cast("int"),
        )
        .otherwise(None)
        .alias("year"),
        col("popularity"),
        col("vote_average").alias("tmdb_rating"),
        col("vote_count").alias("tmdb_votes"),
        col("genre_ids"),
        map_genre_udf(col("genre_ids")).alias("genres_array"),
        col("original_language"),
        col("adult"),
        col("budget"),
        col("revenue"),
    ).withColumn("cleaned_date", current_date())

    with_poster = cleaned.filter(col("poster_url").isNotNull()).count()
    with_year = cleaned.filter(col("year").isNotNull()).count()
    with_genres = cleaned.filter(col("genres_array").isNotNull()).count()

    logger.info(
        f"  With poster URL: {with_poster} ({100 * with_poster / row_count:.1f}%)"
    )
    logger.info(f"  With year: {with_year} ({100 * with_year / row_count:.1f}%)")
    logger.info(f"  With genres: {with_genres} ({100 * with_genres / row_count:.1f}%)")

    output_path = f"{SILVER_TMDB}/movies"
    logger.info(f"  Writing to: {output_path}")
    cleaned.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ movies complete ({row_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean TMDB Data")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting TMDB Silver Cleaning")
    logger.info("=" * 60)

    spark = get_spark_session("Silver-TMDB")

    try:
        clean_tmdb_movies(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("TMDB Silver cleaning complete")
