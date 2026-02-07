from fastapi import APIRouter, Depends, Query
from asyncpg import Connection

from app.database import get_db

router = APIRouter(prefix="/finance", tags=["Finance & Box Office"])


@router.get("/top-revenue")
async def get_top_revenue(
    limit: int = Query(10, ge=1, le=100),
    start_year: int = 1900,
    end_year: int = 2030,
    db: Connection = Depends(get_db),
):
    """Top movies by total revenue"""
    rows = await db.fetch(
        """
        SELECT m.movie_id, m.title, m.year, m.poster_url,
               f.revenue, f.budget,
               (f.revenue - f.budget) AS profit,
               f.imdb_rating
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE f.revenue IS NOT NULL AND f.revenue > 0
          AND m.year BETWEEN $1 AND $2
        ORDER BY f.revenue DESC
        LIMIT $3
        """,
        start_year,
        end_year,
        limit,
    )
    return [dict(r) for r in rows]


@router.get("/top-budget")
async def get_top_budget(
    limit: int = Query(10, ge=1, le=100),
    start_year: int = 1900,
    end_year: int = 2030,
    db: Connection = Depends(get_db),
):
    """Top movies by production budget"""
    rows = await db.fetch(
        """
        SELECT m.movie_id, m.title, m.year, m.poster_url,
               f.budget, f.revenue,
               (f.revenue - f.budget) AS profit,
               f.imdb_rating
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE f.budget IS NOT NULL AND f.budget > 0
          AND m.year BETWEEN $1 AND $2
        ORDER BY f.budget DESC
        LIMIT $3
        """,
        start_year,
        end_year,
        limit,
    )
    return [dict(r) for r in rows]


@router.get("/top-profit")
async def get_top_profit(
    limit: int = Query(10, ge=1, le=100),
    direction: str = Query("best", pattern="^(best|worst)$"),
    start_year: int = 1900,
    end_year: int = 2030,
    db: Connection = Depends(get_db),
):
    """Top movies by profit (revenue - budget). Use direction=worst for biggest flops."""
    order = "DESC" if direction == "best" else "ASC"
    rows = await db.fetch(
        f"""
        SELECT m.movie_id, m.title, m.year, m.poster_url,
               f.budget, f.revenue,
               (f.revenue - f.budget) AS profit,
               f.imdb_rating
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE f.budget IS NOT NULL AND f.budget > 0
          AND f.revenue IS NOT NULL AND f.revenue > 0
          AND m.year BETWEEN $1 AND $2
        ORDER BY profit {order}
        LIMIT $3
        """,
        start_year,
        end_year,
        limit,
    )
    return [dict(r) for r in rows]


@router.get("/roi-leaderboard")
async def get_roi_leaderboard(
    direction: str = Query("best", pattern="^(best|worst)$"),
    min_budget: int = 1_000_000,
    start_year: int = 1980,
    end_year: int = 2025,
    limit: int = Query(25, ge=1, le=100),
    db: Connection = Depends(get_db),
):
    """Movies ranked by ROI (revenue / budget). min_budget filters micro-budget noise."""
    order = "DESC" if direction == "best" else "ASC"
    rows = await db.fetch(
        f"""
        SELECT m.movie_id, m.title, m.year, m.poster_url,
               f.budget, f.revenue,
               (f.revenue - f.budget) AS profit,
               ROUND((f.revenue::numeric / NULLIF(f.budget, 0)), 2) AS revenue_multiple,
               f.imdb_rating
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE f.budget IS NOT NULL AND f.budget >= $1
          AND f.revenue IS NOT NULL AND f.revenue > 0
          AND m.year BETWEEN $2 AND $3
        ORDER BY revenue_multiple {order} NULLS LAST
        LIMIT $4
        """,
        min_budget,
        start_year,
        end_year,
        limit,
    )
    return [dict(r) for r in rows]


@router.get("/genre-profitability")
async def get_genre_profitability(
    start_year: int = 1980,
    end_year: int = 2025,
    min_budget: int = 1_000_000,
    min_movies: int = 20,
    db: Connection = Depends(get_db),
):
    """Genre profitability with median ROI and IQR for risk assessment"""
    rows = await db.fetch(
        """
        WITH g AS (
            SELECT
                bg.genre_id,
                (f.revenue::numeric / NULLIF(f.budget, 0)) AS rm,
                (f.revenue - f.budget) AS profit
            FROM fact_movie_metrics f
            JOIN dim_movie m ON m.movie_id = f.movie_id
            JOIN bridge_movie_genres bg ON bg.movie_id = m.movie_id
            WHERE m.year BETWEEN $1 AND $2
              AND f.budget IS NOT NULL AND f.budget >= $3
              AND f.revenue IS NOT NULL AND f.revenue > 0
        )
        SELECT
            dg.genre_name,
            COUNT(*) AS movie_count,
            ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY rm)::numeric, 2) AS median_roi,
            ROUND(percentile_cont(0.25) WITHIN GROUP (ORDER BY rm)::numeric, 2) AS p25_roi,
            ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY rm)::numeric, 2) AS p75_roi,
            ROUND(AVG(profit)::numeric, 0) AS avg_profit
        FROM g
        JOIN dim_genre dg ON dg.genre_id = g.genre_id
        GROUP BY dg.genre_name
        HAVING COUNT(*) >= $4
        ORDER BY median_roi DESC
        """,
        start_year,
        end_year,
        min_budget,
        min_movies,
    )
    return [dict(r) for r in rows]


@router.get("/profitability-trend")
async def get_profitability_trend(
    start_year: int = 1980,
    end_year: int = 2025,
    min_budget: int = 0,
    db: Connection = Depends(get_db),
):
    """Budget, revenue, and ROI trends by year"""
    rows = await db.fetch(
        """
        SELECT
            m.year,
            COUNT(*) AS movie_count,
            ROUND(AVG(f.budget)::numeric, 0) AS avg_budget,
            ROUND(AVG(f.revenue)::numeric, 0) AS avg_revenue,
            ROUND(AVG(f.revenue - f.budget)::numeric, 0) AS avg_profit,
            ROUND(percentile_cont(0.5) WITHIN GROUP (
                ORDER BY f.revenue::numeric / NULLIF(f.budget, 0)
            )::numeric, 2) AS median_roi
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE m.year BETWEEN $1 AND $2
          AND f.budget IS NOT NULL AND f.budget > 0
          AND f.revenue IS NOT NULL AND f.revenue > 0
          AND ($3 = 0 OR f.budget >= $3)
        GROUP BY m.year
        ORDER BY m.year
        """,
        start_year,
        end_year,
        min_budget,
    )
    return [dict(r) for r in rows]


@router.get("/budget-vs-rating")
async def get_budget_vs_rating(
    source: str = "imdb",
    start_year: int = 1980,
    end_year: int = 2025,
    min_budget: int = 1_000_000,
    sample_size: int = 1000,
    db: Connection = Depends(get_db),
):
    """Scatter data for budget-rating correlation analysis"""
    column_map = {
        "imdb": "f.imdb_rating",
        "tmdb": "f.tmdb_rating",
        "movielens": "f.ml_avg_rating",
    }
    col = column_map.get(source, "f.imdb_rating")

    rows = await db.fetch(
        f"""
        SELECT f.budget, {col} AS rating, f.revenue,
               m.title, m.year
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE f.budget IS NOT NULL AND f.budget >= $1
          AND {col} IS NOT NULL
          AND m.year BETWEEN $2 AND $3
        ORDER BY RANDOM()
        LIMIT $4
        """,
        min_budget,
        start_year,
        end_year,
        sample_size,
    )
    return [dict(r) for r in rows]


@router.get("/star-power-roi")
async def get_star_power_roi(
    category: str = "actor",
    start_year: int = 1980,
    end_year: int = 2025,
    min_budget: int = 1_000_000,
    min_movies: int = 5,
    limit: int = Query(25, ge=1, le=100),
    db: Connection = Depends(get_db),
):
    """Actors/directors ranked by median ROI of their filmography"""
    if category not in ("actor", "director", "writer"):
        category = "actor"

    rows = await db.fetch(
        """
        WITH credits AS (
            SELECT
                p.person_id, p.name,
                (f.revenue::numeric / NULLIF(f.budget, 0)) AS rm,
                (f.revenue - f.budget) AS profit
            FROM bridge_movie_cast bc
            JOIN dim_person p ON p.person_id = bc.person_id
            JOIN fact_movie_metrics f ON f.movie_id = bc.movie_id
            JOIN dim_movie m ON m.movie_id = f.movie_id
            WHERE m.year BETWEEN $1 AND $2
              AND f.budget IS NOT NULL AND f.budget >= $3
              AND f.revenue IS NOT NULL AND f.revenue > 0
              AND bc.category = $4
        )
        SELECT
            person_id, name,
            COUNT(*) AS movie_count,
            ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY rm)::numeric, 2) AS median_roi,
            ROUND(AVG(profit)::numeric, 0) AS avg_profit
        FROM credits
        GROUP BY person_id, name
        HAVING COUNT(*) >= $5
        ORDER BY median_roi DESC
        LIMIT $6
        """,
        start_year,
        end_year,
        min_budget,
        category,
        min_movies,
        limit,
    )
    return [dict(r) for r in rows]


@router.get("/value-frontier")
async def get_value_frontier(
    start_year: int = 1980,
    end_year: int = 2025,
    min_budget: int = 0,
    max_budget: int | None = None,
    min_rating: float | None = None,
    limit: int = Query(50, ge=1, le=100),
    db: Connection = Depends(get_db),
):
    """Movies with best combination of high rating and high ROI (value score)"""
    conditions = [
        "f.budget IS NOT NULL AND f.budget > 0",
        "f.revenue IS NOT NULL AND f.revenue > 0",
        "f.imdb_rating IS NOT NULL",
        "m.year BETWEEN $1 AND $2",
    ]
    params: list = [start_year, end_year]
    idx = 3

    if min_budget > 0:
        conditions.append(f"f.budget >= ${idx}")
        params.append(min_budget)
        idx += 1

    if max_budget is not None:
        conditions.append(f"f.budget <= ${idx}")
        params.append(max_budget)
        idx += 1

    if min_rating is not None:
        conditions.append(f"f.imdb_rating >= ${idx}")
        params.append(min_rating)
        idx += 1

    where = " AND ".join(conditions)

    params.append(limit)
    limit_idx = idx

    rows = await db.fetch(
        f"""
        SELECT m.movie_id, m.title, m.year, m.poster_url,
               f.budget, f.revenue,
               ROUND((f.revenue::numeric / NULLIF(f.budget, 0)), 2) AS revenue_multiple,
               f.imdb_rating,
               ROUND((f.imdb_rating * LN(1 + f.revenue::numeric / NULLIF(f.budget, 0)))::numeric, 2) AS value_score
        FROM dim_movie m
        JOIN fact_movie_metrics f ON m.movie_id = f.movie_id
        WHERE {where}
        ORDER BY value_score DESC NULLS LAST
        LIMIT ${limit_idx}
        """,
        *params,
    )
    return [dict(r) for r in rows]
