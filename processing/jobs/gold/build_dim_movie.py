"""
Gold Layer: Build Movie Dimension

Joins entity spine with all source data to create unified movie dimension.
Source priority per ETL_PLAN.md:
- title: IMDb primaryTitle > MovieLens
- year: IMDb startYear > parsed from ML title
- runtime: IMDb runtimeMinutes
- mpaa_rating: Rotten Tomatoes
- plot_summary: TMDB overview
- poster_url: TMDB (prefixed)

Input: 
- s3://movies-datalake-2310/silver/entity_spine/
- s3://movies-datalake-2310/silver/movielens/movies/
- s3://movies-datalake-2310/silver/imdb/title_basics/
- s3://movies-datalake-2310/silver/tmdb/movies/
- s3://movies-datalake-2310/silver/rotten_tomatoes/movies/

Output: s3://movies-datalake-2310/gold/dim_movie/
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_date

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Gold-DimMovie")

S3_BUCKET = "movies-datalake-2310"
SILVER_PATH = f"s3://{S3_BUCKET}/silver"
GOLD_PATH = f"s3://{S3_BUCKET}/gold"


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def build_dim_movie(spark: SparkSession):
    """Build movie dimension by joining entity spine with all sources"""
    logger.info("Building movie dimension")

    spine_path = f"{SILVER_PATH}/entity_spine"
    logger.info(f"  Reading entity spine from: {spine_path}")
    spine_df = spark.read.parquet(spine_path)
    spine_count = spine_df.count()
    logger.info(f"    Entity spine records: {spine_count}")

    ml_path = f"{SILVER_PATH}/movielens/movies"
    logger.info(f"  Reading MovieLens from: {ml_path}")
    ml_df = spark.read.parquet(ml_path).select(
        col("ml_id"),
        col("title").alias("ml_title"),
        col("year").alias("ml_year"),
        col("genres_array").alias("ml_genres"),
    )
    ml_count = ml_df.count()
    logger.info(f"    MovieLens records: {ml_count}")

    imdb_path = f"{SILVER_PATH}/imdb/title_basics"
    logger.info(f"  Reading IMDb from: {imdb_path}")
    imdb_df = spark.read.parquet(imdb_path).select(
        col("imdb_id"),
        col("primary_title").alias("imdb_title"),
        col("original_title").alias("imdb_original_title"),
        col("year").alias("imdb_year"),
        col("runtime_minutes").alias("runtime"),
        col("is_adult"),
        col("genres_array").alias("imdb_genres"),
    )
    imdb_count = imdb_df.count()
    logger.info(f"    IMDb records: {imdb_count}")

    tmdb_path = f"{SILVER_PATH}/tmdb/movies"
    logger.info(f"  Reading TMDB from: {tmdb_path}")
    tmdb_df = spark.read.parquet(tmdb_path).select(
        col("tmdb_id"),
        col("title").alias("tmdb_title"),
        col("plot_summary"),
        col("poster_url"),
        col("backdrop_url"),
        col("popularity").alias("tmdb_popularity"),
        col("year").alias("tmdb_year"),
        col("genres_array").alias("tmdb_genres"),
    )
    tmdb_count = tmdb_df.count()
    logger.info(f"    TMDB records: {tmdb_count}")

    rt_path = f"{SILVER_PATH}/rotten_tomatoes/movies"
    logger.info(f"  Reading Rotten Tomatoes from: {rt_path}")
    rt_df = spark.read.parquet(rt_path).select(
        col("rt_slug"),
        col("title").alias("rt_title"),
        col("mpaa_rating"),
        col("release_year").alias("rt_year"),
        col("genres_array").alias("rt_genres"),
    )
    rt_count = rt_df.count()
    logger.info(f"    Rotten Tomatoes records: {rt_count}")

    logger.info("  Joining sources...")

    joined = spine_df.join(ml_df, on="ml_id", how="left")
    joined = joined.join(imdb_df, on="imdb_id", how="left")
    joined = joined.join(tmdb_df, on="tmdb_id", how="left")
    joined = joined.join(rt_df, on="rt_slug", how="left")

    dim_movie = joined.select(
        col("movie_id"),
        col("imdb_id"),
        col("tmdb_id"),
        col("ml_id"),
        col("rt_slug"),
        coalesce(col("imdb_title"), col("ml_title"), col("tmdb_title"), col("rt_title")).alias("title"),
        coalesce(col("imdb_original_title"), col("tmdb_title")).alias("original_title"),
        coalesce(col("imdb_year"), col("ml_year"), col("tmdb_year"), col("rt_year")).alias("year"),
        col("runtime"),
        col("mpaa_rating"),
        col("plot_summary"),
        col("poster_url"),
        col("backdrop_url"),
        col("is_adult"),
        col("tmdb_popularity").alias("popularity"),
    ).withColumn("created_date", current_date())

    dim_movie = dim_movie.dropDuplicates(["movie_id"])

    final_count = dim_movie.count()
    with_title = dim_movie.filter(col("title").isNotNull()).count()
    with_year = dim_movie.filter(col("year").isNotNull()).count()
    with_runtime = dim_movie.filter(col("runtime").isNotNull()).count()
    with_poster = dim_movie.filter(col("poster_url").isNotNull()).count()
    with_plot = dim_movie.filter(col("plot_summary").isNotNull()).count()
    with_mpaa = dim_movie.filter(col("mpaa_rating").isNotNull()).count()

    logger.info("=" * 60)
    logger.info("Movie Dimension Statistics")
    logger.info("=" * 60)
    logger.info(f"  Total movies: {final_count}")
    logger.info(f"  With title: {with_title} ({100 * with_title / final_count:.1f}%)")
    logger.info(f"  With year: {with_year} ({100 * with_year / final_count:.1f}%)")
    logger.info(f"  With runtime: {with_runtime} ({100 * with_runtime / final_count:.1f}%)")
    logger.info(f"  With poster: {with_poster} ({100 * with_poster / final_count:.1f}%)")
    logger.info(f"  With plot: {with_plot} ({100 * with_plot / final_count:.1f}%)")
    logger.info(f"  With MPAA rating: {with_mpaa} ({100 * with_mpaa / final_count:.1f}%)")

    output_path = f"{GOLD_PATH}/dim_movie"
    logger.info(f"  Writing to: {output_path}")
    dim_movie.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ dim_movie complete ({final_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Movie Dimension")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting Movie Dimension Build")
    logger.info("=" * 60)

    spark = get_spark_session("Gold-DimMovie")

    try:
        build_dim_movie(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("Movie dimension build complete")
