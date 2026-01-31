from fastapi import APIRouter, Depends
from asyncpg import Connection

from app.database import get_db

router = APIRouter(prefix="/temporal", tags=["Temporal & Meta Features"])


@router.get("/runtime-trend")
async def get_runtime_trend(
    start_year: int = 1950,
    end_year: int = 2025,
    db: Connection = Depends(get_db),
):
    """Average runtime trend by year"""
    rows = await db.fetch(
        """
        SELECT 
            year,
            AVG(runtime) as avg_runtime,
            COUNT(*) as movie_count
        FROM dim_movie
        WHERE runtime IS NOT NULL 
          AND runtime BETWEEN 30 AND 300
          AND year BETWEEN $1 AND $2
        GROUP BY year
        ORDER BY year
        """,
        start_year,
        end_year,
    )
    return [dict(r) for r in rows]


@router.get("/quality-by-month")
async def get_quality_by_month(db: Connection = Depends(get_db)):
    """Average rating and count by release month (Oscar season analysis)"""
    rows = await db.fetch(
        """
        SELECT 
            t.month,
            AVG(f.imdb_rating) as avg_rating,
            COUNT(*) as movie_count
        FROM fact_movie_metrics f
        JOIN dim_time t ON f.time_id = t.time_id
        WHERE f.imdb_rating IS NOT NULL AND t.month IS NOT NULL
        GROUP BY t.month
        ORDER BY t.month
        """
    )
    return [dict(r) for r in rows]


@router.get("/mpaa-distribution")
async def get_mpaa_distribution(db: Connection = Depends(get_db)):
    """Distribution by MPAA rating (G, PG, PG-13, R, NC-17)"""
    rows = await db.fetch(
        """
        SELECT 
            mpaa_rating,
            COUNT(*) as count,
            AVG(f.imdb_rating) as avg_rating
        FROM dim_movie m
        LEFT JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE mpaa_rating IS NOT NULL
        GROUP BY mpaa_rating
        ORDER BY count DESC
        """
    )
    return [dict(r) for r in rows]


@router.get("/mpaa-trend")
async def get_mpaa_trend(db: Connection = Depends(get_db)):
    """MPAA rating trend by decade"""
    rows = await db.fetch(
        """
        SELECT 
            (year / 10) * 10 as decade,
            mpaa_rating,
            COUNT(*) as count
        FROM dim_movie
        WHERE mpaa_rating IS NOT NULL AND year >= 1960
        GROUP BY decade, mpaa_rating
        ORDER BY decade, count DESC
        """
    )
    return [dict(r) for r in rows]
