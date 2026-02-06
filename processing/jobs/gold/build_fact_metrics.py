"""
Gold Layer: Build Fact Movie Metrics

Aggregates all ratings and metrics from multiple sources into unified fact table.
Includes: MovieLens avg/count, IMDb rating/votes, RT tomatometer/audience/divisive,
          TMDB budget/revenue

Input:
- s3://movies-datalake-2310/silver/entity_spine/
- s3://movies-datalake-2310/silver/movielens/ratings_agg/
- s3://movies-datalake-2310/silver/imdb/title_ratings/
- s3://movies-datalake-2310/silver/rotten_tomatoes/movies/
- s3://movies-datalake-2310/silver/tmdb/movies/

Output: s3://movies-datalake-2310/gold/fact_movie_metrics/
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    lit,
    round as spark_round,
    xxhash64,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Gold-FactMetrics")

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


def build_fact_metrics(spark: SparkSession):
    """Build fact table with all movie metrics"""
    logger.info("Building fact_movie_metrics")

    spine_path = f"{SILVER_PATH}/entity_spine"
    logger.info(f"  Reading entity spine from: {spine_path}")
    spine_df = spark.read.parquet(spine_path).select(
        "movie_id", "ml_id", "imdb_id", "tmdb_id", "rt_slug"
    )
    spine_count = spine_df.count()
    logger.info(f"    Entity spine records: {spine_count}")

    ml_ratings_path = f"{SILVER_PATH}/movielens/ratings_agg"
    logger.info(f"  Reading MovieLens ratings from: {ml_ratings_path}")
    ml_ratings = spark.read.parquet(ml_ratings_path).select(
        col("ml_id"),
        spark_round(col("ml_avg_rating"), 2).alias("ml_avg_rating"),
        col("ml_rating_count"),
    )
    ml_count = ml_ratings.count()
    logger.info(f"    MovieLens aggregated ratings: {ml_count}")

    imdb_ratings_path = f"{SILVER_PATH}/imdb/title_ratings"
    logger.info(f"  Reading IMDb ratings from: {imdb_ratings_path}")
    imdb_ratings = spark.read.parquet(imdb_ratings_path).select(
        col("imdb_id"),
        col("imdb_rating"),
        col("imdb_votes"),
    )
    imdb_count = imdb_ratings.count()
    logger.info(f"    IMDb ratings: {imdb_count}")

    rt_path = f"{SILVER_PATH}/rotten_tomatoes/movies"
    logger.info(f"  Reading Rotten Tomatoes from: {rt_path}")
    rt_df = spark.read.parquet(rt_path).select(
        col("rt_slug"),
        col("tomatometer_score"),
        col("audience_score"),
        col("divisive_score"),
    )
    rt_count = rt_df.count()
    logger.info(f"    Rotten Tomatoes records: {rt_count}")

    tmdb_path = f"{SILVER_PATH}/tmdb/movies"
    logger.info(f"  Reading TMDB from: {tmdb_path}")
    tmdb_df = spark.read.parquet(tmdb_path).select(
        col("tmdb_id"),
        col("tmdb_rating"),
        col("tmdb_votes"),
        col("popularity").alias("tmdb_popularity"),
        col("budget"),
        col("revenue"),
    )
    tmdb_count = tmdb_df.count()
    logger.info(f"    TMDB records: {tmdb_count}")

    logger.info("  Joining sources...")

    joined = spine_df.join(ml_ratings, on="ml_id", how="left")
    joined = joined.join(imdb_ratings, on="imdb_id", how="left")
    joined = joined.join(rt_df, on="rt_slug", how="left")
    joined = joined.join(tmdb_df, on="tmdb_id", how="left")

    today_int = int(spark.sql("SELECT date_format(current_date(), 'yyyyMMdd')").collect()[0][0])

    fact_metrics = joined.select(
        xxhash64(col("movie_id"), lit(today_int)).alias("metric_id"),
        col("movie_id"),
        lit(today_int).alias("time_id"),
        col("ml_avg_rating"),
        col("ml_rating_count"),
        col("imdb_rating"),
        col("imdb_votes"),
        col("tmdb_rating"),
        col("tmdb_votes"),
        col("tmdb_popularity"),
        col("tomatometer_score"),
        col("audience_score"),
        col("divisive_score"),
        col("budget"),
        col("revenue"),
    ).withColumn("created_date", current_date())

    fact_metrics = fact_metrics.dropDuplicates(["movie_id"])

    final_count = fact_metrics.count()

    with_ml = fact_metrics.filter(col("ml_avg_rating").isNotNull()).count()
    with_imdb = fact_metrics.filter(col("imdb_rating").isNotNull()).count()
    with_tmdb = fact_metrics.filter(col("tmdb_rating").isNotNull()).count()
    with_tomatometer = fact_metrics.filter(col("tomatometer_score").isNotNull()).count()
    with_audience = fact_metrics.filter(col("audience_score").isNotNull()).count()
    with_divisive = fact_metrics.filter(col("divisive_score").isNotNull()).count()
    with_budget = fact_metrics.filter(col("budget").isNotNull()).count()
    with_revenue = fact_metrics.filter(col("revenue").isNotNull()).count()

    logger.info("=" * 60)
    logger.info("Fact Movie Metrics Statistics")
    logger.info("=" * 60)
    logger.info(f"  Total records: {final_count}")
    logger.info(f"  With MovieLens rating: {with_ml} ({100 * with_ml / final_count:.1f}%)")
    logger.info(f"  With IMDb rating: {with_imdb} ({100 * with_imdb / final_count:.1f}%)")
    logger.info(f"  With TMDB rating: {with_tmdb} ({100 * with_tmdb / final_count:.1f}%)")
    logger.info(f"  With Tomatometer: {with_tomatometer} ({100 * with_tomatometer / final_count:.1f}%)")
    logger.info(f"  With Audience Score: {with_audience} ({100 * with_audience / final_count:.1f}%)")
    logger.info(f"  With Divisive Score: {with_divisive} ({100 * with_divisive / final_count:.1f}%)")
    logger.info(f"  With Budget: {with_budget} ({100 * with_budget / final_count:.1f}%)")
    logger.info(f"  With Revenue: {with_revenue} ({100 * with_revenue / final_count:.1f}%)")

    output_path = f"{GOLD_PATH}/fact_movie_metrics"
    logger.info(f"  Writing to: {output_path}")
    fact_metrics.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ fact_movie_metrics complete ({final_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Fact Movie Metrics")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting Fact Movie Metrics Build")
    logger.info("=" * 60)

    spark = get_spark_session("Gold-FactMetrics")

    try:
        build_fact_metrics(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("Fact movie metrics build complete")
