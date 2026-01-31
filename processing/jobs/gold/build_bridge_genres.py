"""
Gold Layer: Build Bridge Movie Genres

Creates many-to-many relationship between movies and genres.
Merges genres from all sources (MovieLens, IMDb, TMDB, RT) per movie.

Input:
- s3://movies-datalake-2310/silver/entity_spine/
- s3://movies-datalake-2310/silver/movielens/movies/
- s3://movies-datalake-2310/silver/imdb/title_basics/
- s3://movies-datalake-2310/silver/tmdb/movies/
- s3://movies-datalake-2310/silver/rotten_tomatoes/movies/
- s3://movies-datalake-2310/gold/dim_genre/

Output: s3://movies-datalake-2310/gold/bridge_movie_genres/
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, explode, trim

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Gold-BridgeGenres")

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


def build_bridge_genres(spark: SparkSession):
    """Build bridge table between movies and genres"""
    logger.info("Building bridge_movie_genres")

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    normalize_genre_udf = udf(normalize_genre, StringType())

    spine_path = f"{SILVER_PATH}/entity_spine"
    logger.info(f"  Reading entity spine from: {spine_path}")
    spine_df = spark.read.parquet(spine_path).select(
        "movie_id", "ml_id", "imdb_id", "tmdb_id", "rt_slug"
    )
    spine_count = spine_df.count()
    logger.info(f"    Entity spine records: {spine_count}")

    dim_genre_path = f"{GOLD_PATH}/dim_genre"
    logger.info(f"  Reading dim_genre from: {dim_genre_path}")
    dim_genre = spark.read.parquet(dim_genre_path).select("genre_id", "genre_name")
    genre_count = dim_genre.count()
    logger.info(f"    Genre dimension records: {genre_count}")

    all_movie_genres = []

    ml_path = f"{SILVER_PATH}/movielens/movies"
    logger.info(f"  Reading MovieLens genres from: {ml_path}")
    try:
        ml_df = spark.read.parquet(ml_path).select("ml_id", "genres_array")
        ml_genres = (
            ml_df.select(
                col("ml_id"),
                explode(col("genres_array")).alias("genre"),
            )
            .filter(col("genre").isNotNull())
            .select(
                col("ml_id"),
                normalize_genre_udf(col("genre")).alias("genre_name"),
            )
            .filter(col("genre_name").isNotNull())
        )
        ml_with_spine = spine_df.select("movie_id", "ml_id").join(ml_genres, on="ml_id", how="inner")
        all_movie_genres.append(ml_with_spine.select("movie_id", "genre_name"))
        logger.info(f"    MovieLens genres extracted")
    except Exception as e:
        logger.warning(f"    Could not read MovieLens: {e}")

    imdb_path = f"{SILVER_PATH}/imdb/title_basics"
    logger.info(f"  Reading IMDb genres from: {imdb_path}")
    try:
        imdb_df = spark.read.parquet(imdb_path).select("imdb_id", "genres_array")
        imdb_genres = (
            imdb_df.select(
                col("imdb_id"),
                explode(col("genres_array")).alias("genre"),
            )
            .filter(col("genre").isNotNull())
            .select(
                col("imdb_id"),
                normalize_genre_udf(trim(col("genre"))).alias("genre_name"),
            )
            .filter(col("genre_name").isNotNull())
        )
        imdb_with_spine = spine_df.select("movie_id", "imdb_id").join(imdb_genres, on="imdb_id", how="inner")
        all_movie_genres.append(imdb_with_spine.select("movie_id", "genre_name"))
        logger.info(f"    IMDb genres extracted")
    except Exception as e:
        logger.warning(f"    Could not read IMDb: {e}")

    tmdb_path = f"{SILVER_PATH}/tmdb/movies"
    logger.info(f"  Reading TMDB genres from: {tmdb_path}")
    try:
        tmdb_df = spark.read.parquet(tmdb_path).select("tmdb_id", "genres_array")
        tmdb_genres = (
            tmdb_df.select(
                col("tmdb_id"),
                explode(col("genres_array")).alias("genre"),
            )
            .filter(col("genre").isNotNull())
            .select(
                col("tmdb_id"),
                normalize_genre_udf(col("genre")).alias("genre_name"),
            )
            .filter(col("genre_name").isNotNull())
        )
        tmdb_with_spine = spine_df.select("movie_id", "tmdb_id").join(tmdb_genres, on="tmdb_id", how="inner")
        all_movie_genres.append(tmdb_with_spine.select("movie_id", "genre_name"))
        logger.info(f"    TMDB genres extracted")
    except Exception as e:
        logger.warning(f"    Could not read TMDB: {e}")

    rt_path = f"{SILVER_PATH}/rotten_tomatoes/movies"
    logger.info(f"  Reading Rotten Tomatoes genres from: {rt_path}")
    try:
        rt_df = spark.read.parquet(rt_path).select("rt_slug", "genres_array")
        rt_genres = (
            rt_df.select(
                col("rt_slug"),
                explode(col("genres_array")).alias("genre"),
            )
            .filter(col("genre").isNotNull())
            .select(
                col("rt_slug"),
                normalize_genre_udf(trim(col("genre"))).alias("genre_name"),
            )
            .filter(col("genre_name").isNotNull())
        )
        rt_with_spine = spine_df.select("movie_id", "rt_slug").join(rt_genres, on="rt_slug", how="inner")
        all_movie_genres.append(rt_with_spine.select("movie_id", "genre_name"))
        logger.info(f"    Rotten Tomatoes genres extracted")
    except Exception as e:
        logger.warning(f"    Could not read Rotten Tomatoes: {e}")

    if not all_movie_genres:
        raise ValueError("No genre data found from any source")

    combined = all_movie_genres[0]
    for df in all_movie_genres[1:]:
        combined = combined.union(df)

    combined = combined.distinct()

    bridge_genres = combined.join(dim_genre, on="genre_name", how="inner")

    bridge_genres = bridge_genres.select(
        col("movie_id"),
        col("genre_id"),
    ).distinct().withColumn("created_date", current_date())

    final_count = bridge_genres.count()
    unique_movies = bridge_genres.select("movie_id").distinct().count()
    unique_genres = bridge_genres.select("genre_id").distinct().count()

    logger.info("=" * 60)
    logger.info("Bridge Movie Genres Statistics")
    logger.info("=" * 60)
    logger.info(f"  Total records: {final_count}")
    logger.info(f"  Unique movies: {unique_movies}")
    logger.info(f"  Unique genres: {unique_genres}")
    logger.info(f"  Avg genres per movie: {final_count / unique_movies:.1f}")

    output_path = f"{GOLD_PATH}/bridge_movie_genres"
    logger.info(f"  Writing to: {output_path}")
    bridge_genres.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ bridge_movie_genres complete ({final_count} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Bridge Movie Genres")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Starting Bridge Movie Genres Build")
    logger.info("=" * 60)

    spark = get_spark_session("Gold-BridgeGenres")

    try:
        build_bridge_genres(spark)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

    logger.info("Bridge movie genres build complete")
