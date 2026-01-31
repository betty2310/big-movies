from fastapi import APIRouter, Depends
from asyncpg import Connection

from app.database import get_db

router = APIRouter(prefix="/people", tags=["People Analytics"])


@router.get("/top-prolific")
async def get_top_prolific(
    category: str = "actor",
    limit: int = 20,
    db: Connection = Depends(get_db),
):
    """Top actors/directors by film count (most prolific)"""
    rows = await db.fetch(
        """
        SELECT 
            p.person_id,
            p.name,
            COUNT(DISTINCT bc.movie_id) as movie_count
        FROM dim_person p
        JOIN bridge_movie_cast bc ON p.person_id = bc.person_id
        WHERE bc.category = $1
        GROUP BY p.person_id, p.name
        ORDER BY movie_count DESC
        LIMIT $2
        """,
        category,
        limit,
    )
    return [dict(r) for r in rows]


@router.get("/top-rated")
async def get_top_rated(
    category: str = "actor",
    min_films: int = 5,
    limit: int = 20,
    db: Connection = Depends(get_db),
):
    """Top actors/directors by average rating (min films filter)"""
    rows = await db.fetch(
        """
        SELECT 
            p.person_id,
            p.name,
            AVG(f.imdb_rating) as avg_rating,
            COUNT(DISTINCT bc.movie_id) as movie_count
        FROM dim_person p
        JOIN bridge_movie_cast bc ON p.person_id = bc.person_id
        JOIN fact_movie_metrics f ON bc.movie_id = f.movie_id
        WHERE bc.category = $1 AND f.imdb_rating IS NOT NULL
        GROUP BY p.person_id, p.name
        HAVING COUNT(DISTINCT bc.movie_id) >= $2
        ORDER BY avg_rating DESC
        LIMIT $3
        """,
        category,
        min_films,
        limit,
    )
    return [dict(r) for r in rows]


@router.get("/rating-history/{person_id}")
async def get_person_rating_history(
    person_id: str,
    db: Connection = Depends(get_db),
):
    """Person's rating history over years (for sparkline)"""
    rows = await db.fetch(
        """
        SELECT 
            m.year,
            AVG(f.imdb_rating) as avg_rating,
            COUNT(*) as movie_count
        FROM bridge_movie_cast bc
        JOIN dim_movie m ON bc.movie_id = m.movie_id
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE bc.person_id = $1 AND f.imdb_rating IS NOT NULL
        GROUP BY m.year
        ORDER BY m.year
        """,
        person_id,
    )
    return [dict(r) for r in rows]


@router.get("/actor-network")
async def get_actor_network(
    min_collaborations: int = 3,
    limit: int = 100,
    db: Connection = Depends(get_db),
):
    """Actor collaboration network (who works together frequently)"""
    rows = await db.fetch(
        """
        SELECT 
            p1.name as actor1,
            p2.name as actor2,
            COUNT(*) as collaborations
        FROM bridge_movie_cast bc1
        JOIN bridge_movie_cast bc2 ON bc1.movie_id = bc2.movie_id 
            AND bc1.person_id < bc2.person_id
        JOIN dim_person p1 ON bc1.person_id = p1.person_id
        JOIN dim_person p2 ON bc2.person_id = p2.person_id
        WHERE bc1.category = 'actor' AND bc2.category = 'actor'
        GROUP BY p1.name, p2.name
        HAVING COUNT(*) >= $1
        ORDER BY collaborations DESC
        LIMIT $2
        """,
        min_collaborations,
        limit,
    )
    return [dict(r) for r in rows]


@router.get("/{person_id}")
async def get_person_detail(
    person_id: str,
    db: Connection = Depends(get_db),
):
    """Get person details with filmography"""
    person = await db.fetchrow(
        "SELECT * FROM dim_person WHERE person_id = $1",
        person_id,
    )
    if not person:
        return None

    films = await db.fetch(
        """
        SELECT m.movie_id, m.title, m.year, bc.category, bc.characters,
               f.imdb_rating
        FROM bridge_movie_cast bc
        JOIN dim_movie m ON bc.movie_id = m.movie_id
        LEFT JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE bc.person_id = $1
        ORDER BY m.year DESC
        """,
        person_id,
    )
    return {"person": dict(person), "filmography": [dict(f) for f in films]}
