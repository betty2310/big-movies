"""
Silver Layer: Build Entity Resolution Spine

Multi-pass entity resolution across 4 data sources:
  1. Deterministic joins: MovieLens links -> TMDB (tmdb_id) + IMDb (imdb_id)
  2. Canonical title/year from TMDB > IMDb > MovieLens
  3. RT matching: exact canonical title+year, then relaxed year (±1)
  4. Orphan entities: TMDB-only, IMDb-only, RT-only rows not in MovieLens

Input:
  - s3://movies-datalake-2310/bronze/movielens/links/
  - s3://movies-datalake-2310/bronze/movielens/movies/
  - s3://movies-datalake-2310/bronze/tmdb/movies/
  - s3://movies-datalake-2310/bronze/imdb/title_basics/
  - s3://movies-datalake-2310/bronze/rotten_tomatoes/movies/
Output:
  - s3://movies-datalake-2310/silver/entity_spine/
"""

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
)
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    count,
    current_date,
    lit,
    lower,
    lpad,
    monotonically_increasing_id,
    regexp_extract,
    regexp_replace,
    row_number,
    to_date,
    trim,
    when,
)
from pyspark.sql.functions import (
    year as spark_year,
)
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Silver-EntitySpine")

S3_BUCKET = "movies-datalake-2310"
BRONZE_MOVIELENS = f"s3://{S3_BUCKET}/bronze/movielens"
BRONZE_TMDB = f"s3://{S3_BUCKET}/bronze/tmdb"
BRONZE_IMDB = f"s3://{S3_BUCKET}/bronze/imdb"
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
    normalized = lower(trim(title_col))
    normalized = regexp_replace(normalized, r"[^\w\s]", "")
    normalized = regexp_replace(normalized, r"\s+", " ")
    normalized = regexp_replace(normalized, r"\s*\(\d{4}\)\s*$", "")
    return trim(normalized)


def log_stats(label: str, df: DataFrame, id_cols: dict):
    total = df.count()
    logger.info(f"  {label}: {total} total")
    for name, col_name in id_cols.items():
        has = df.filter(col(col_name).isNotNull()).count()
        pct = 100 * has / total if total > 0 else 0
        logger.info(f"    With {name}: {has} ({pct:.1f}%)")


def build_entity_spine(spark: SparkSession):
    logger.info("=" * 60)
    logger.info("Building Entity Resolution Spine (Multi-Pass)")
    logger.info("=" * 60)

    # ── Load all bronze sources ──────────────────────────────────
    logger.info("Loading bronze data sources...")

    links_df = spark.read.parquet(f"{BRONZE_MOVIELENS}/links").select(
        col("movieId").alias("ml_id"),
        col("imdbId"),
        col("tmdbId").alias("tmdb_id"),
    )
    links_df = links_df.withColumn(
        "imdb_id",
        when(
            col("imdbId").isNotNull(),
            concat(lit("tt"), lpad(col("imdbId").cast("string"), 7, "0")),
        ),
    ).drop("imdbId")

    ml_movies_df = spark.read.parquet(f"{BRONZE_MOVIELENS}/movies").select(
        col("movieId").alias("ml_id"),
        col("title").alias("ml_title"),
    )
    ml_movies_df = ml_movies_df.withColumn(
        "ml_clean_title",
        regexp_replace(col("ml_title"), r"\s*\(\d{4}\)\s*$", ""),
    ).withColumn(
        "ml_year",
        regexp_extract(col("ml_title"), r"\((\d{4})\)\s*$", 1).cast("int"),
    )

    tmdb_df = spark.read.parquet(f"{BRONZE_TMDB}/movies").select(
        col("id").alias("tmdb_id"),
        col("title").alias("tmdb_title"),
        col("original_title").alias("tmdb_original_title"),
        col("release_date").alias("tmdb_release_date"),
    )
    tmdb_df = tmdb_df.withColumn(
        "tmdb_year", spark_year(to_date(col("tmdb_release_date"), "yyyy-MM-dd"))
    )

    imdb_df = spark.read.parquet(f"{BRONZE_IMDB}/title_basics").select(
        col("tconst").alias("imdb_id"),
        col("primaryTitle").alias("imdb_title"),
        col("startYear").alias("imdb_year_str"),
        col("titleType"),
    )
    imdb_df = (
        imdb_df.filter(col("titleType") == "movie")
        .withColumn("imdb_year", col("imdb_year_str").cast("int"))
        .drop("titleType", "imdb_year_str")
    )

    rt_df = spark.read.parquet(f"{BRONZE_RT}/movies").select(
        col("rt_id"),
        col("slug").alias("rt_slug"),
        col("title").alias("rt_title"),
        col("release_year").alias("rt_year"),
    )

    total_links = links_df.count()
    total_tmdb = tmdb_df.count()
    total_imdb = imdb_df.count()
    total_rt = rt_df.count()

    logger.info(f"  MovieLens links: {total_links}")
    logger.info(f"  TMDB movies: {total_tmdb}")
    logger.info(f"  IMDb movies (titleType=movie): {total_imdb}")
    logger.info(f"  Rotten Tomatoes: {total_rt}")

    # ── Pass 1: Deterministic joins via MovieLens links ──────────
    logger.info("=" * 60)
    logger.info("Pass 1: Deterministic joins (ML links -> TMDB + IMDb)")
    logger.info("=" * 60)

    core = links_df.join(ml_movies_df, on="ml_id", how="left")
    core = core.join(tmdb_df, on="tmdb_id", how="left")
    core = core.join(imdb_df, on="imdb_id", how="left")

    tmdb_joined = core.filter(col("tmdb_title").isNotNull()).count()
    imdb_joined = core.filter(col("imdb_title").isNotNull()).count()
    logger.info(f"  ML->TMDB joined: {tmdb_joined}/{total_links}")
    logger.info(f"  ML->IMDb joined: {imdb_joined}/{total_links}")

    # ── Canonical title + year (TMDB > IMDb > MovieLens) ─────────
    logger.info("Computing canonical title/year (TMDB > IMDb > MovieLens)")

    core = core.withColumn(
        "canonical_title",
        coalesce(col("tmdb_title"), col("imdb_title"), col("ml_clean_title")),
    ).withColumn(
        "canonical_year",
        coalesce(col("tmdb_year"), col("imdb_year"), col("ml_year")),
    )

    core = core.withColumn("normalized_title", normalize_title(col("canonical_title")))

    # ── Pass 2: RT matching against canonical title+year ─────────
    logger.info("=" * 60)
    logger.info("Pass 2: Rotten Tomatoes matching (multi-pass)")
    logger.info("=" * 60)

    rt_normalized = rt_df.withColumn(
        "rt_normalized_title", normalize_title(col("rt_title"))
    )

    # Pass 2a: exact title + year
    logger.info("  Pass 2a: Exact canonical title + year match")
    exact_match = core.join(
        rt_normalized,
        (core["normalized_title"] == rt_normalized["rt_normalized_title"]),
        how="inner",
    ).select(
        core["ml_id"],
        rt_normalized["rt_slug"],
        rt_normalized["rt_id"],
        lit("title_year_exact").alias("rt_match_method"),
    )

    w_rt = Window.partitionBy("ml_id").orderBy("rt_slug")
    exact_match = (
        exact_match.withColumn("_rn", row_number().over(w_rt))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    w_rt_slug = Window.partitionBy("rt_slug").orderBy("ml_id")
    exact_match = (
        exact_match.withColumn("_rn", row_number().over(w_rt_slug))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    exact_count = exact_match.count()
    logger.info(f"    Exact matches: {exact_count}")

    matched_rt_slugs = exact_match.select("rt_slug")
    matched_ml_ids = exact_match.select("ml_id")
    remaining_rt = rt_normalized.join(
        matched_rt_slugs, on="rt_slug", how="left_anti"
    ).select(
        col("rt_slug").alias("rem_rt_slug"),
        col("rt_id").alias("rem_rt_id"),
        col("rt_year").alias("rem_rt_year"),
        col("rt_normalized_title").alias("rem_rt_normalized_title"),
    )
    remaining_core = core.join(matched_ml_ids, on="ml_id", how="left_anti")

    # Pass 2b: title match with year ±1
    logger.info("  Pass 2b: Canonical title + year ±1 match")
    fuzzy_year_match = remaining_core.join(
        remaining_rt,
        (remaining_core["normalized_title"] == remaining_rt["rem_rt_normalized_title"])
        & (
            spark_abs(remaining_core["canonical_year"] - remaining_rt["rem_rt_year"])
            <= 1
        )
        & remaining_core["canonical_year"].isNotNull()
        & remaining_rt["rem_rt_year"].isNotNull(),
        how="inner",
    ).select(
        remaining_core["ml_id"],
        col("rem_rt_slug").alias("rt_slug"),
        col("rem_rt_id").alias("rt_id"),
        lit("title_year_fuzzy").alias("rt_match_method"),
    )

    w_rt2 = Window.partitionBy("ml_id").orderBy("rt_slug")
    fuzzy_year_match = (
        fuzzy_year_match.withColumn("_rn", row_number().over(w_rt2))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    w_rt_slug2 = Window.partitionBy("rt_slug").orderBy("ml_id")
    fuzzy_year_match = (
        fuzzy_year_match.withColumn("_rn", row_number().over(w_rt_slug2))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    fuzzy_count = fuzzy_year_match.count()
    logger.info(f"    Fuzzy year matches: {fuzzy_count}")

    all_rt_matches = exact_match.unionByName(fuzzy_year_match)

    core_with_rt = core.join(all_rt_matches, on="ml_id", how="left")

    core_final = core_with_rt.select(
        "ml_id", "imdb_id", "tmdb_id", "rt_slug", "rt_id", "rt_match_method"
    )

    # ── Pass 3: Orphan entities ──────────────────────────────────
    logger.info("=" * 60)
    logger.info("Pass 3: Adding orphan entities")
    logger.info("=" * 60)

    # TMDB-only (not in MovieLens links)
    linked_tmdb = links_df.select("tmdb_id").filter(col("tmdb_id").isNotNull())
    tmdb_orphans = tmdb_df.join(linked_tmdb, on="tmdb_id", how="left_anti").select(
        lit(None).cast("int").alias("ml_id"),
        lit(None).cast("string").alias("imdb_id"),
        col("tmdb_id"),
        lit(None).cast("string").alias("rt_slug"),
        lit(None).cast("string").alias("rt_id"),
        lit(None).cast("string").alias("rt_match_method"),
    )
    tmdb_orphan_count = tmdb_orphans.count()
    logger.info(f"  TMDB-only orphans: {tmdb_orphan_count}")

    # IMDb-only (not in MovieLens links)
    linked_imdb = links_df.select("imdb_id").filter(col("imdb_id").isNotNull())
    imdb_orphans = imdb_df.join(linked_imdb, on="imdb_id", how="left_anti").select(
        lit(None).cast("int").alias("ml_id"),
        col("imdb_id"),
        lit(None).cast("int").alias("tmdb_id"),
        lit(None).cast("string").alias("rt_slug"),
        lit(None).cast("string").alias("rt_id"),
        lit(None).cast("string").alias("rt_match_method"),
    )
    imdb_orphan_count = imdb_orphans.count()
    logger.info(f"  IMDb-only orphans: {imdb_orphan_count}")

    # RT-only (not matched to any entity)
    all_matched_rt = all_rt_matches.select("rt_slug")
    rt_orphans = rt_df.join(all_matched_rt, on="rt_slug", how="left_anti").select(
        lit(None).cast("int").alias("ml_id"),
        lit(None).cast("string").alias("imdb_id"),
        lit(None).cast("int").alias("tmdb_id"),
        col("rt_slug"),
        col("rt_id"),
        lit("unmatched").alias("rt_match_method"),
    )
    rt_orphan_count = rt_orphans.count()
    logger.info(f"  RT-only orphans: {rt_orphan_count}")

    # ── Union all entities ───────────────────────────────────────
    all_entities = (
        core_final.unionByName(tmdb_orphans)
        .unionByName(imdb_orphans)
        .unionByName(rt_orphans)
    )

    # ── Serial movie_id starting from 1 ────────────────────────
    w_id = Window.orderBy(
        when(col("imdb_id").isNotNull(), 0)
        .when(col("tmdb_id").isNotNull(), 1)
        .when(col("ml_id").isNotNull(), 2)
        .otherwise(3),
        "imdb_id",
        "tmdb_id",
        "ml_id",
        "rt_slug",
    )
    all_entities = all_entities.withColumn("movie_id", row_number().over(w_id))

    final_spine = all_entities.select(
        "movie_id",
        "ml_id",
        "imdb_id",
        "tmdb_id",
        "rt_slug",
        "rt_match_method",
    ).withColumn("created_date", current_date())

    final_spine = final_spine.dropDuplicates(["movie_id"])

    # ── Final statistics ─────────────────────────────────────────
    final_count = final_spine.count()
    has_ml = final_spine.filter(col("ml_id").isNotNull()).count()
    has_imdb = final_spine.filter(col("imdb_id").isNotNull()).count()
    has_tmdb = final_spine.filter(col("tmdb_id").isNotNull()).count()
    has_rt = final_spine.filter(col("rt_slug").isNotNull()).count()

    rt_matched = final_spine.filter(
        col("rt_match_method").isNotNull() & (col("rt_match_method") != "unmatched")
    ).count()

    logger.info("=" * 60)
    logger.info("Final Entity Spine Summary")
    logger.info("=" * 60)
    logger.info(f"  Total entities: {final_count}")
    logger.info(f"  With MovieLens ID: {has_ml}")
    logger.info(f"  With IMDb ID: {has_imdb}")
    logger.info(f"  With TMDB ID: {has_tmdb}")
    logger.info(f"  With RT slug: {has_rt}")
    logger.info(f"  RT matched (title_year_exact): {exact_count}")
    logger.info(f"  RT matched (title_year_fuzzy): {fuzzy_count}")
    logger.info(f"  RT unmatched (orphans): {rt_orphan_count}")

    match_methods = (
        final_spine.groupBy("rt_match_method").agg(count("*").alias("cnt")).collect()
    )
    logger.info("  RT match method breakdown:")
    for row in match_methods:
        method = row["rt_match_method"] if row["rt_match_method"] else "none"
        logger.info(f"    {method}: {row['cnt']}")

    output_path = SILVER_SPINE
    logger.info(f"Writing entity spine to: {output_path}")
    final_spine.write.mode("overwrite").parquet(output_path)
    logger.info("Entity spine complete")

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
