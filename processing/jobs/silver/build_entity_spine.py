"""
Silver Layer: Build Entity Resolution Spine

Creates unified movie_id mapping from MovieLens links.csv:
- ml_id (MovieLens) -> imdb_id (with tt prefix) -> tmdb_id -> rt_slug

Input: s3://movies-datalake-2310/bronze/movielens/links/
Output: s3://movies-datalake-2310/silver/entity_spine/
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    current_date,
    lit,
    lower,
    lpad,
    regexp_replace,
    trim,
    when,
    xxhash64,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Silver-EntitySpine")

S3_BUCKET = "movies-datalake-2310"
BRONZE_MOVIELENS = f"s3://{S3_BUCKET}/bronze/movielens"
BRONZE_RT = f"s3://{S3_BUCKET}/bronze/rotten_tomatoes"
SILVER_SPINE = f"s3://{S3_BUCKET}/silver/entity_spine"


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def normalize_title(title_col):
    """Normalize title for matching: lowercase, remove punctuation, collapse whitespace"""
    normalized = lower(trim(title_col))
    normalized = regexp_replace(normalized, r"[^\w\s]", "")
    normalized = regexp_replace(normalized, r"\s+", " ")
    normalized = regexp_replace(normalized, r"\s*\(\d{4}\)\s*$", "")
    return trim(normalized)


def build_entity_spine(spark: SparkSession):
    logger.info("=" * 60)
    logger.info("Building Entity Resolution Spine")
    logger.info("=" * 60)

    links_path = f"{BRONZE_MOVIELENS}/links"
    movies_path = f"{BRONZE_MOVIELENS}/movies"
    rt_path = f"{BRONZE_RT}/movies"

    logger.info(f"Reading MovieLens links from: {links_path}")
    links_df = spark.read.parquet(links_path)
    total_links = links_df.count()
    logger.info(f"  Total link records: {total_links}")

    logger.info(f"Reading MovieLens movies from: {movies_path}")
    movies_df = spark.read.parquet(movies_path)

    logger.info(f"Reading Rotten Tomatoes movies from: {rt_path}")
    rt_df = spark.read.parquet(rt_path)
    total_rt = rt_df.count()
    logger.info(f"  Total RT records: {total_rt}")

    spine_df = links_df.select(
        col("movieId").alias("ml_id"),
        col("imdbId"),
        col("tmdbId").alias("tmdb_id"),
    )

    spine_df = spine_df.withColumn(
        "imdb_id",
        when(
            col("imdbId").isNotNull(),
            concat(lit("tt"), lpad(col("imdbId").cast("string"), 7, "0")),
        ).otherwise(None),
    ).drop("imdbId")

    has_imdb = spine_df.filter(col("imdb_id").isNotNull()).count()
    has_tmdb = spine_df.filter(col("tmdb_id").isNotNull()).count()
    has_both = spine_df.filter(
        col("imdb_id").isNotNull() & col("tmdb_id").isNotNull()
    ).count()

    logger.info("=" * 60)
    logger.info("Entity Mapping Statistics (MovieLens Links)")
    logger.info("=" * 60)
    logger.info(f"  Total MovieLens movies: {total_links}")
    logger.info(f"  With IMDb ID: {has_imdb} ({100 * has_imdb / total_links:.1f}%)")
    logger.info(f"  With TMDB ID: {has_tmdb} ({100 * has_tmdb / total_links:.1f}%)")
    logger.info(f"  With both IDs: {has_both} ({100 * has_both / total_links:.1f}%)")

    movies_with_year = movies_df.select(
        col("movieId").alias("ml_id"),
        col("title").alias("ml_title"),
        regexp_replace(col("title"), r"\s*\(\d{4}\)\s*$", "").alias("clean_title"),
    ).withColumn("normalized_title", normalize_title(col("clean_title")))

    rt_normalized = rt_df.select(
        col("rt_id"),
        col("slug").alias("rt_slug"),
        col("title").alias("rt_title"),
        col("release_year").alias("rt_year"),
    ).withColumn("normalized_title", normalize_title(col("rt_title")))

    spine_with_title = spine_df.join(movies_with_year, on="ml_id", how="left")

    from pyspark.sql.functions import regexp_extract

    spine_with_title = spine_with_title.withColumn(
        "ml_year", regexp_extract(col("ml_title"), r"\((\d{4})\)\s*$", 1).cast("int")
    )

    rt_matched = spine_with_title.join(
        rt_normalized,
        (spine_with_title["normalized_title"] == rt_normalized["normalized_title"])
        & (spine_with_title["ml_year"] == rt_normalized["rt_year"]),
        how="left",
    ).select(
        spine_with_title["ml_id"],
        spine_with_title["imdb_id"],
        spine_with_title["tmdb_id"],
        rt_normalized["rt_slug"],
        when(rt_normalized["rt_slug"].isNotNull(), lit("title_year_match"))
        .otherwise(None)
        .alias("rt_match_method"),
    )

    rt_matched = rt_matched.dropDuplicates(["ml_id"])

    final_spine = rt_matched.withColumn(
        "movie_id",
        xxhash64(
            when(col("imdb_id").isNotNull(), col("imdb_id"))
            .when(col("tmdb_id").isNotNull(), col("tmdb_id").cast("string"))
            .otherwise(col("ml_id").cast("string"))
        ),
    )

    final_spine = final_spine.select(
        "movie_id",
        "ml_id",
        "imdb_id",
        "tmdb_id",
        "rt_slug",
        "rt_match_method",
    ).withColumn("created_date", current_date())

    has_rt = final_spine.filter(col("rt_slug").isNotNull()).count()
    final_count = final_spine.count()

    logger.info("=" * 60)
    logger.info("Rotten Tomatoes Matching Statistics")
    logger.info("=" * 60)
    logger.info(
        f"  RT records matched: {has_rt} of {total_rt} ({100 * has_rt / total_rt:.1f}%)"
    )
    logger.info(f"  RT records unmatched: {total_rt - has_rt}")

    logger.info("=" * 60)
    logger.info("Final Entity Spine Summary")
    logger.info("=" * 60)
    logger.info(f"  Total entities: {final_count}")
    logger.info(f"  With IMDb ID: {has_imdb}")
    logger.info(f"  With TMDB ID: {has_tmdb}")
    logger.info(f"  With RT slug: {has_rt}")

    output_path = SILVER_SPINE
    logger.info(f"Writing entity spine to: {output_path}")
    final_spine.write.mode("overwrite").parquet(output_path)
    logger.info("✓ Entity spine complete")

    return final_spine


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Entity Resolution Spine")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting Entity Spine Build")
    logger.info("=" * 60)

    spark = get_spark_session("Silver-EntitySpine")

    try:
        build_entity_spine(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("Entity Spine job complete")
