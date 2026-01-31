from fastapi import APIRouter, Depends, Query
from asyncpg import Connection

from app.database import get_db

router = APIRouter(prefix="/movies", tags=["Movies"])


@router.get("")
async def get_movies(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    year: int | None = None,
    genre_id: int | None = None,
    min_rating: float | None = None,
    search: str | None = None,
    sort_by: str = "year",
    sort_order: str = "desc",
    db: Connection = Depends(get_db),
):
    """List movies with pagination and filters"""
    offset = (page - 1) * limit
    
    conditions = []
    params = []
    param_idx = 1
    
    if year:
        conditions.append(f"m.year = ${param_idx}")
        params.append(year)
        param_idx += 1
    
    if min_rating:
        conditions.append(f"f.imdb_rating >= ${param_idx}")
        params.append(min_rating)
        param_idx += 1
    
    if search:
        conditions.append(f"m.title ILIKE ${param_idx}")
        params.append(f"%{search}%")
        param_idx += 1
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    sort_columns = {
        "year": "m.year",
        "title": "m.title",
        "rating": "f.imdb_rating",
        "votes": "f.imdb_votes",
    }
    sort_col = sort_columns.get(sort_by, "m.year")
    sort_dir = "DESC" if sort_order.lower() == "desc" else "ASC"
    
    query = f"""
        SELECT m.movie_id, m.title, m.year, m.runtime, m.poster_url,
               f.imdb_rating, f.imdb_votes, f.tmdb_rating
        FROM dim_movie m
        LEFT JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        {where_clause}
        ORDER BY {sort_col} {sort_dir} NULLS LAST
        LIMIT ${param_idx} OFFSET ${param_idx + 1}
    """
    params.extend([limit, offset])
    
    rows = await db.fetch(query, *params)
    
    count_query = f"""
        SELECT COUNT(*) FROM dim_movie m
        LEFT JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        {where_clause}
    """
    total = await db.fetchval(count_query, *params[:-2]) if params[:-2] else await db.fetchval(count_query)
    
    return {
        "data": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.get("/{movie_id}")
async def get_movie_detail(
    movie_id: int,
    db: Connection = Depends(get_db),
):
    """Get movie details with cast, genres, and metrics"""
    movie = await db.fetchrow(
        """
        SELECT m.*, f.ml_avg_rating, f.ml_rating_count,
               f.imdb_rating, f.imdb_votes, f.tmdb_rating, f.tmdb_votes,
               f.tomatometer_score, f.audience_score, f.divisive_score
        FROM dim_movie m
        LEFT JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE m.movie_id = $1
        """,
        movie_id,
    )
    if not movie:
        return None
    
    genres = await db.fetch(
        """
        SELECT g.genre_id, g.genre_name
        FROM bridge_movie_genres bg
        JOIN dim_genre g ON bg.genre_id = g.genre_id
        WHERE bg.movie_id = $1
        """,
        movie_id,
    )
    
    cast = await db.fetch(
        """
        SELECT p.person_id, p.name, bc.category, bc.ordering, bc.characters
        FROM bridge_movie_cast bc
        JOIN dim_person p ON bc.person_id = p.person_id
        WHERE bc.movie_id = $1
        ORDER BY bc.ordering
        LIMIT 20
        """,
        movie_id,
    )
    
    return {
        "movie": dict(movie),
        "genres": [dict(g) for g in genres],
        "cast": [dict(c) for c in cast],
    }
