from asyncpg import Connection
from fastapi import APIRouter, Depends

from app.database import get_db

router = APIRouter(prefix="/genres", tags=["Genre Evolution"])


@router.get("/share-by-decade")
async def get_genre_share_by_decade(db: Connection = Depends(get_db)):
    """Genre market share by decade (for stacked area chart)"""
    rows = await db.fetch(
        """
        SELECT
            (m.year / 10) * 10 as decade,
            g.genre_name,
            COUNT(*) as count
        FROM dim_movie m
        JOIN bridge_movie_genres bg ON m.movie_id = bg.movie_id
        JOIN dim_genre g ON bg.genre_id = g.genre_id
        WHERE m.year >= 1920 AND m.year <= 2024
        GROUP BY decade, g.genre_name
        ORDER BY decade, count DESC
        """
    )
    return [dict(r) for r in rows]


@router.get("/average-rating")
async def get_genre_average_rating(db: Connection = Depends(get_db)):
    """Average rating by genre"""
    rows = await db.fetch(
        """
        SELECT
            g.genre_name,
            AVG(f.imdb_rating) as avg_rating,
            COUNT(*) as movie_count
        FROM dim_genre g
        JOIN bridge_movie_genres bg ON g.genre_id = bg.genre_id
        JOIN fact_movie_metrics f ON bg.movie_id = f.movie_id
        WHERE f.imdb_rating IS NOT NULL
        GROUP BY g.genre_name
        HAVING COUNT(*) > 100
        ORDER BY avg_rating DESC
        """
    )
    return [dict(r) for r in rows]


@router.get("/co-occurrence")
async def get_genre_cooccurrence(db: Connection = Depends(get_db)):
    """Genre co-occurrence matrix (which genres pair together)"""
    rows = await db.fetch(
        """
        SELECT
            g1.genre_name as genre1,
            g2.genre_name as genre2,
            COUNT(*) as count
        FROM bridge_movie_genres bg1
        JOIN bridge_movie_genres bg2 ON bg1.movie_id = bg2.movie_id
            AND bg1.genre_id < bg2.genre_id
        JOIN dim_genre g1 ON bg1.genre_id = g1.genre_id
        JOIN dim_genre g2 ON bg2.genre_id = g2.genre_id
        GROUP BY g1.genre_name, g2.genre_name
        HAVING COUNT(*) > 50
        ORDER BY count DESC
        """
    )
    return [dict(r) for r in rows]


@router.get("/list")
async def get_all_genres(db: Connection = Depends(get_db)):
    """List all genres"""
    rows = await db.fetch(
        """
        SELECT g.genre_id, g.genre_name, COUNT(bg.movie_id) as movie_count
        FROM dim_genre g
        LEFT JOIN bridge_movie_genres bg ON g.genre_id = bg.genre_id
        GROUP BY g.genre_id, g.genre_name
        ORDER BY movie_count DESC
        """
    )
    return [dict(r) for r in rows]
