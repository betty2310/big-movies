from fastapi import APIRouter, Depends
from asyncpg import Connection

from app.database import get_db

router = APIRouter(prefix="/ratings", tags=["Ratings & Reception"])


@router.get("/distribution")
async def get_rating_distribution(
    source: str = "imdb",
    db: Connection = Depends(get_db),
):
    """Rating distribution histogram (bins 0-1, 1-2, ..., 9-10)"""
    column_map = {
        "imdb": "imdb_rating",
        "tmdb": "tmdb_rating",
        "movielens": "ml_avg_rating",
    }
    column = column_map.get(source, "imdb_rating")

    rows = await db.fetch(
        f"""
        SELECT 
            FLOOR({column})::int as bin,
            COUNT(*) as count
        FROM fact_movie_metrics
        WHERE {column} IS NOT NULL
        GROUP BY bin
        ORDER BY bin
        """
    )
    return [dict(r) for r in rows]


@router.get("/platform-comparison")
async def get_platform_comparison(
    start_year: int = 1990,
    end_year: int = 2025,
    db: Connection = Depends(get_db),
):
    """Compare average ratings across platforms by year"""
    rows = await db.fetch(
        """
        SELECT 
            m.year,
            AVG(f.imdb_rating) as imdb_avg,
            AVG(f.tmdb_rating) as tmdb_avg,
            AVG(f.ml_avg_rating) as ml_avg
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE m.year BETWEEN $1 AND $2
        GROUP BY m.year
        ORDER BY m.year
        """,
        start_year,
        end_year,
    )
    return [dict(r) for r in rows]


@router.get("/cult-classics")
async def get_cult_classics(
    min_rating: float = 8.0,
    max_votes: int = 10000,
    limit: int = 50,
    db: Connection = Depends(get_db),
):
    """Hidden gems: high rating but low vote count"""
    rows = await db.fetch(
        """
        SELECT m.movie_id, m.title, m.year, m.poster_url,
               f.imdb_rating, f.imdb_votes, f.tmdb_rating
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE f.imdb_rating >= $1 
          AND f.imdb_votes <= $2
          AND f.imdb_votes > 0
        ORDER BY f.imdb_rating DESC, f.imdb_votes ASC
        LIMIT $3
        """,
        min_rating,
        max_votes,
        limit,
    )
    return [dict(r) for r in rows]


@router.get("/runtime-vs-rating")
async def get_runtime_vs_rating(
    sample_size: int = 1000,
    db: Connection = Depends(get_db),
):
    """Correlation between runtime and rating"""
    rows = await db.fetch(
        """
        SELECT m.runtime, f.imdb_rating as rating
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE m.runtime IS NOT NULL 
          AND m.runtime BETWEEN 30 AND 300
          AND f.imdb_rating IS NOT NULL
        ORDER BY RANDOM()
        LIMIT $1
        """,
        sample_size,
    )
    return [dict(r) for r in rows]
