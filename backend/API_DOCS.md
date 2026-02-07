# Big Movies API Documentation

REST API for movie analytics data warehouse. Built with FastAPI and PostgreSQL (Supabase).

## Base URL

```
http://localhost:8000/api
```

## Quick Start

```bash
# Install dependencies
uv sync

# Set environment variables
cp .env.example .env
# Edit .env with your DATABASE_URL

# Run server
uv run uvicorn app.main:app --reload
```

API docs available at: `http://localhost:8000/docs`

---

## 1. Market Overview

### Get Movies Per Year

Returns movie count by year for volume trend analysis.

```
GET /api/overview/movies-per-year
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start_year` | int | 1900 | Start year filter |
| `end_year` | int | 2030 | End year filter |

**Response:**

```json
[
  { "year": 2020, "count": 1234 },
  { "year": 2021, "count": 1456 }
]
```

---

### Get Top Popular Movies

Returns top movies by popularity metrics.

```
GET /api/overview/top-popular
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 10 | Number of results |
| `metric` | string | "imdb_votes" | Sort by: `imdb_votes`, `tmdb_popularity`, `tmdb_votes` |

**Response:**

```json
[
  {
    "movie_id": 1,
    "title": "The Shawshank Redemption",
    "year": 1994,
    "poster_url": "https://...",
    "imdb_votes": 2500000,
    "tmdb_popularity": 98.5,
    "imdb_rating": 9.3,
    "budget": 63000000,
    "revenue": 28341469
  }
]
```

---

### Get Language Distribution

Returns movie count by detected language (based on original_title patterns).

```
GET /api/overview/language-distribution
```

**Response:**

```json
[
  { "language": "English", "count": 45000 },
  { "language": "French", "count": 5000 },
  { "language": "Japanese", "count": 3500 }
]
```

---

## 2. Ratings & Reception

### Get Rating Distribution

Returns histogram data for rating distribution.

```
GET /api/ratings/distribution
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | string | "imdb" | Rating source: `imdb`, `tmdb`, `movielens` |

**Response:**

```json
[
  { "bin": 5, "count": 8500 },
  { "bin": 6, "count": 15000 },
  { "bin": 7, "count": 22000 }
]
```

---

### Get Platform Comparison

Compares average ratings across platforms by year.

```
GET /api/ratings/platform-comparison
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start_year` | int | 1990 | Start year |
| `end_year` | int | 2025 | End year |

**Response:**

```json
[
  {
    "year": 2020,
    "imdb_avg": 6.5,
    "tmdb_avg": 6.8,
    "ml_avg": 3.4
  }
]
```

---

### Get Cult Classics

Returns hidden gems with high ratings but low vote counts.

```
GET /api/ratings/cult-classics
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_rating` | float | 8.0 | Minimum IMDb rating |
| `max_votes` | int | 10000 | Maximum vote count |
| `limit` | int | 50 | Number of results |

**Response:**

```json
[
  {
    "movie_id": 123,
    "title": "Hidden Gem Movie",
    "year": 2005,
    "poster_url": "https://...",
    "imdb_rating": 8.5,
    "imdb_votes": 2500,
    "tmdb_rating": 8.2
  }
]
```

---

### Get Runtime vs Rating

Returns scatter plot data for runtime-rating correlation.

```
GET /api/ratings/runtime-vs-rating
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sample_size` | int | 1000 | Number of random samples |

**Response:**

```json
[
  { "runtime": 120, "rating": 7.5 },
  { "runtime": 95, "rating": 6.8 }
]
```

---

## 3. Genre Evolution

### Get Genre Share by Decade

Returns genre distribution by decade for stacked area chart.

```
GET /api/genres/share-by-decade
```

**Response:**

```json
[
  { "decade": 1990, "genre_name": "Drama", "count": 5000 },
  { "decade": 1990, "genre_name": "Comedy", "count": 4500 },
  { "decade": 2000, "genre_name": "Action", "count": 6000 }
]
```

---

### Get Genre Average Rating

Returns average rating by genre.

```
GET /api/genres/average-rating
```

**Response:**

```json
[
  { "genre_name": "Documentary", "avg_rating": 7.2, "movie_count": 5000 },
  { "genre_name": "Drama", "avg_rating": 6.8, "movie_count": 25000 }
]
```

---

### Get Genre Co-occurrence

Returns genre pairing data for heatmap visualization.

```
GET /api/genres/co-occurrence
```

**Response:**

```json
[
  { "genre1": "Action", "genre2": "Adventure", "count": 8500 },
  { "genre1": "Comedy", "genre2": "Romance", "count": 7200 }
]
```

---

### Get All Genres

Returns list of all genres with movie counts.

```
GET /api/genres/list
```

**Response:**

```json
[
  { "genre_id": 1, "genre_name": "Drama", "movie_count": 25000 },
  { "genre_id": 2, "genre_name": "Comedy", "movie_count": 18000 }
]
```

---

## 4. People Analytics

### Get Top Prolific

Returns most prolific actors/directors by film count.

```
GET /api/people/top-prolific
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `category` | string | "actor" | Person type: `actor`, `director`, `writer` |
| `limit` | int | 20 | Number of results |

**Response:**

```json
[
  { "person_id": "nm0000001", "name": "Samuel L. Jackson", "movie_count": 150 }
]
```

---

### Get Top Rated

Returns highest-rated actors/directors (minimum film threshold).

```
GET /api/people/top-rated
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `category` | string | "actor" | Person type: `actor`, `director` |
| `min_films` | int | 5 | Minimum films to qualify |
| `limit` | int | 20 | Number of results |

**Response:**

```json
[
  {
    "person_id": "nm0000001",
    "name": "Christopher Nolan",
    "avg_rating": 8.2,
    "movie_count": 12
  }
]
```

---

### Get Person Rating History

Returns a person's rating history over years (for sparkline charts).

```
GET /api/people/rating-history/{person_id}
```

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `person_id` | string | IMDb person ID (e.g., "nm0000001") |

**Response:**

```json
[
  { "year": 2015, "avg_rating": 7.5, "movie_count": 3 },
  { "year": 2018, "avg_rating": 8.1, "movie_count": 2 }
]
```

---

### Get Actor Network

Returns actor collaboration data for network graph visualization.

```
GET /api/people/actor-network
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_collaborations` | int | 3 | Minimum shared movies |
| `limit` | int | 100 | Number of edges |

**Response:**

```json
[
  { "actor1": "Tom Hanks", "actor2": "Meg Ryan", "collaborations": 4 },
  { "actor1": "Leonardo DiCaprio", "actor2": "Kate Winslet", "collaborations": 3 }
]
```

---

### Get Person Detail

Returns person details with full filmography.

```
GET /api/people/{person_id}
```

**Response:**

```json
{
  "person": {
    "person_id": "nm0000001",
    "name": "Tom Hanks",
    "birth_year": 1956,
    "primary_profession": "actor"
  },
  "filmography": [
    {
      "movie_id": 123,
      "title": "Forrest Gump",
      "year": 1994,
      "category": "actor",
      "characters": "[\"Forrest Gump\"]",
      "imdb_rating": 8.8
    }
  ]
}
```

---

## 5. Temporal & Meta Features

### Get Runtime Trend

Returns average runtime by year.

```
GET /api/temporal/runtime-trend
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start_year` | int | 1950 | Start year |
| `end_year` | int | 2025 | End year |

**Response:**

```json
[
  { "year": 2020, "avg_runtime": 105.5, "movie_count": 1200 },
  { "year": 2021, "avg_runtime": 108.2, "movie_count": 1100 }
]
```

---

### Get Quality by Month

Returns average rating by release month (Oscar season analysis).

```
GET /api/temporal/quality-by-month
```

**Response:**

```json
[
  { "month": 1, "avg_rating": 6.2, "movie_count": 5000 },
  { "month": 12, "avg_rating": 7.1, "movie_count": 8000 }
]
```

---

### Get MPAA Distribution

Returns movie distribution by MPAA rating.

```
GET /api/temporal/mpaa-distribution
```

**Response:**

```json
[
  { "mpaa_rating": "R", "count": 25000, "avg_rating": 6.5 },
  { "mpaa_rating": "PG-13", "count": 18000, "avg_rating": 6.2 }
]
```

---

### Get MPAA Trend

Returns MPAA rating distribution by decade.

```
GET /api/temporal/mpaa-trend
```

**Response:**

```json
[
  { "decade": 2000, "mpaa_rating": "R", "count": 8000 },
  { "decade": 2000, "mpaa_rating": "PG-13", "count": 6500 }
]
```

---

## 6. Finance & Box Office

### Get Top Revenue Movies

Returns top movies by total revenue.

```
GET /api/finance/top-revenue
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 10 | Number of results (max 100) |
| `start_year` | int | 1900 | Start year filter |
| `end_year` | int | 2030 | End year filter |

**Response:**

```json
[
  {
    "movie_id": 1,
    "title": "Avatar",
    "year": 2009,
    "poster_url": "https://...",
    "revenue": 2923706026,
    "budget": 237000000,
    "profit": 2686706026,
    "imdb_rating": 7.9
  }
]
```

---

### Get Top Budget Movies

Returns top movies by production budget.

```
GET /api/finance/top-budget
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 10 | Number of results (max 100) |
| `start_year` | int | 1900 | Start year filter |
| `end_year` | int | 2030 | End year filter |

**Response:**

```json
[
  {
    "movie_id": 1,
    "title": "Pirates of the Caribbean: On Stranger Tides",
    "year": 2011,
    "poster_url": "https://...",
    "budget": 379000000,
    "revenue": 1045713802,
    "profit": 666713802,
    "imdb_rating": 6.6
  }
]
```

---

### Get Top Profit Movies

Returns top movies by profit (revenue - budget). Use `direction=worst` for biggest flops.

```
GET /api/finance/top-profit
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 10 | Number of results (max 100) |
| `direction` | string | "best" | `best` for most profitable, `worst` for biggest flops |
| `start_year` | int | 1900 | Start year filter |
| `end_year` | int | 2030 | End year filter |

**Response:**

```json
[
  {
    "movie_id": 1,
    "title": "Avatar",
    "year": 2009,
    "poster_url": "https://...",
    "budget": 237000000,
    "revenue": 2923706026,
    "profit": 2686706026,
    "imdb_rating": 7.9
  }
]
```

---

### Get ROI Leaderboard

Returns movies ranked by return on investment (revenue / budget).

```
GET /api/finance/roi-leaderboard
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `direction` | string | "best" | `best` or `worst` |
| `min_budget` | int | 1000000 | Minimum budget filter (avoids micro-budget noise) |
| `start_year` | int | 1980 | Start year |
| `end_year` | int | 2025 | End year |
| `limit` | int | 25 | Number of results (max 100) |

**Response:**

```json
[
  {
    "movie_id": 1,
    "title": "Paranormal Activity",
    "year": 2007,
    "poster_url": "https://...",
    "budget": 15000,
    "revenue": 193355800,
    "profit": 193340800,
    "revenue_multiple": 12890.39,
    "imdb_rating": 6.3
  }
]
```

---

### Get Genre Profitability

Returns genre profitability with median ROI and interquartile range for risk assessment.

```
GET /api/finance/genre-profitability
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start_year` | int | 1980 | Start year |
| `end_year` | int | 2025 | End year |
| `min_budget` | int | 1000000 | Minimum budget filter |
| `min_movies` | int | 20 | Minimum movies to qualify |

**Response:**

```json
[
  {
    "genre_name": "Horror",
    "movie_count": 350,
    "median_roi": 3.45,
    "p25_roi": 1.20,
    "p75_roi": 7.80,
    "avg_profit": 45000000
  }
]
```

---

### Get Profitability Trend

Returns budget, revenue, and ROI trends by year.

```
GET /api/finance/profitability-trend
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start_year` | int | 1980 | Start year |
| `end_year` | int | 2025 | End year |
| `min_budget` | int | 0 | Minimum budget filter |

**Response:**

```json
[
  {
    "year": 2020,
    "movie_count": 150,
    "avg_budget": 45000000,
    "avg_revenue": 120000000,
    "avg_profit": 75000000,
    "median_roi": 2.15
  }
]
```

---

### Get Budget vs Rating

Returns scatter plot data for budget-rating correlation analysis.

```
GET /api/finance/budget-vs-rating
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | string | "imdb" | Rating source: `imdb`, `tmdb`, `movielens` |
| `start_year` | int | 1980 | Start year |
| `end_year` | int | 2025 | End year |
| `min_budget` | int | 1000000 | Minimum budget |
| `sample_size` | int | 1000 | Number of random samples |

**Response:**

```json
[
  {
    "budget": 160000000,
    "rating": 8.8,
    "revenue": 836836967,
    "title": "Inception",
    "year": 2010
  }
]
```

---

### Get Star Power ROI

Returns actors/directors ranked by median ROI of their filmography.

```
GET /api/finance/star-power-roi
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `category` | string | "actor" | Person type: `actor`, `director`, `writer` |
| `start_year` | int | 1980 | Start year |
| `end_year` | int | 2025 | End year |
| `min_budget` | int | 1000000 | Minimum budget filter |
| `min_movies` | int | 5 | Minimum films to qualify |
| `limit` | int | 25 | Number of results (max 100) |

**Response:**

```json
[
  {
    "person_id": "nm0000229",
    "name": "Steven Spielberg",
    "movie_count": 30,
    "median_roi": 4.52,
    "avg_profit": 250000000
  }
]
```

---

### Get Value Frontier

Returns movies with the best combination of high rating and high ROI (Pareto-optimal value).

```
GET /api/finance/value-frontier
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start_year` | int | 1980 | Start year |
| `end_year` | int | 2025 | End year |
| `min_budget` | int | 0 | Minimum budget |
| `max_budget` | int | - | Maximum budget (optional) |
| `min_rating` | float | - | Minimum IMDb rating (optional) |
| `limit` | int | 50 | Number of results (max 100) |

**Response:**

```json
[
  {
    "movie_id": 1,
    "title": "The Blair Witch Project",
    "year": 1999,
    "poster_url": "https://...",
    "budget": 60000,
    "revenue": 248639099,
    "revenue_multiple": 4143.98,
    "imdb_rating": 6.5,
    "value_score": 53.12
  }
]
```

---

## 7. Movies

### List Movies

Returns paginated movie list with filters.

```
GET /api/movies
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | int | 1 | Page number |
| `limit` | int | 20 | Results per page (max 100) |
| `year` | int | - | Filter by year |
| `genre_id` | int | - | Filter by genre |
| `min_rating` | float | - | Minimum IMDb rating |
| `search` | string | - | Search by title |
| `sort_by` | string | "year" | Sort: `year`, `title`, `rating`, `votes` |
| `sort_order` | string | "desc" | Order: `asc`, `desc` |

**Response:**

```json
{
  "data": [
    {
      "movie_id": 1,
      "title": "Inception",
      "year": 2010,
      "runtime": 148,
      "poster_url": "https://...",
      "imdb_rating": 8.8,
      "imdb_votes": 2200000,
      "tmdb_rating": 8.4,
      "budget": 160000000,
      "revenue": 836836967
    }
  ],
  "total": 50000,
  "page": 1,
  "limit": 20
}
```

---

### Get Movie Detail

Returns full movie details with cast and genres.

```
GET /api/movies/{movie_id}
```

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `movie_id` | int | Movie ID |

**Response:**

```json
{
  "movie": {
    "movie_id": 1,
    "title": "Inception",
    "original_title": "Inception",
    "year": 2010,
    "runtime": 148,
    "mpaa_rating": "PG-13",
    "plot_summary": "A thief who steals...",
    "poster_url": "https://...",
    "imdb_rating": 8.8,
    "imdb_votes": 2200000,
    "tmdb_rating": 8.4,
    "tomatometer_score": 87,
    "audience_score": 91,
    "budget": 160000000,
    "revenue": 836836967
  },
  "genres": [
    { "genre_id": 1, "genre_name": "Action" },
    { "genre_id": 2, "genre_name": "Sci-Fi" }
  ],
  "cast": [
    {
      "person_id": "nm0000138",
      "name": "Leonardo DiCaprio",
      "category": "actor",
      "ordering": 1,
      "characters": "[\"Cobb\"]"
    }
  ]
}
```

---

## 8. Error Responses

All endpoints return standard HTTP error codes:

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request |
| 404 | Not Found |
| 500 | Internal Server Error |

Error response format:

```json
{
  "detail": "Error message description"
}
```
