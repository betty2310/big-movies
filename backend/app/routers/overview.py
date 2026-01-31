from asyncpg import Connection
from fastapi import APIRouter, Depends

from app.database import get_db

router = APIRouter(prefix="/overview", tags=["Market Overview"])


@router.get("/movies-per-year")
async def get_movies_per_year(
    start_year: int = 1900,
    end_year: int = 2030,
    db: Connection = Depends(get_db),
):
    """Movies released per year (Volume Trend)"""
    rows = await db.fetch(
        """
        SELECT year, COUNT(*) as count
        FROM dim_movie
        WHERE year BETWEEN $1 AND $2
        GROUP BY year
        ORDER BY year
        """,
        start_year,
        end_year,
    )
    return [dict(r) for r in rows]


@router.get("/top-popular")
async def get_top_popular(
    limit: int = 10,
    metric: str = "imdb_votes",
    db: Connection = Depends(get_db),
):
    """Top movies by popularity (IMDb votes or TMDB popularity)"""
    if metric not in ("imdb_votes", "tmdb_popularity", "tmdb_votes"):
        metric = "imdb_votes"

    rows = await db.fetch(
        f"""
        SELECT m.movie_id, m.title, m.year, m.poster_url,
               f.imdb_votes, f.tmdb_popularity, f.imdb_rating
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE f.{metric} IS NOT NULL
        ORDER BY f.{metric} DESC
        LIMIT $1
        """,
        limit,
    )
    return [{**dict(r), "movie_id": str(r["movie_id"])} for r in rows]


@router.get("/language-distribution")
async def get_language_distribution(db: Connection = Depends(get_db)):
    """Geographic/language distribution based on original_title patterns"""
    rows = await db.fetch(
        """
        SELECT
            CASE
                WHEN original_title ~ '^[A-Za-z0-9 .,!?''"-]+$' THEN 'English'
                WHEN original_title ~ '[\u4e00-\u9fff]' THEN 'Chinese'
                WHEN original_title ~ '[\u3040-\u30ff]' THEN 'Japanese'
                WHEN original_title ~ '[\u0400-\u04ff]' THEN 'Russian'
                WHEN original_title ~ '[àâäéèêëïîôùûüÿœæç]' THEN 'French'
                WHEN original_title ~ '[áéíóúüñ¿¡]' THEN 'Spanish'
                WHEN original_title ~ '[äöüß]' THEN 'German'
                WHEN original_title ~ '[\uac00-\ud7af]' THEN 'Korean'
                ELSE 'Other'
            END as language,
            COUNT(*) as count
        FROM dim_movie
        WHERE original_title IS NOT NULL
        GROUP BY language
        ORDER BY count DESC
        """
    )
    return [dict(r) for r in rows]
