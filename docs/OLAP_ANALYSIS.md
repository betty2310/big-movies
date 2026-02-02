# OLAP Analysis in Big Movies Data Warehouse

## Star Schema Overview

```
                    ┌─────────────┐
                    │  dim_time   │
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴────────┐    ┌─────────────┐
│  dim_genre  │────│ fact_movie_    │────│  dim_movie  │
└─────────────┘    │    metrics     │    └─────────────┘
                   └───────┬────────┘
                           │
                    ┌──────┴──────┐
                    │ dim_person  │
                    └─────────────┘
```

**Fact Table:** `fact_movie_metrics` - Contains measurable metrics (ratings, votes, revenue)

**Dimension Tables:**
- `dim_time` - Time hierarchy (year, quarter, month, day)
- `dim_movie` - Movie attributes
- `dim_genre` - Genre classification
- `dim_person` - Cast/crew information

**Bridge Tables:** `bridge_movie_cast`, `bridge_movie_genres` (many-to-many relationships)

---

## What is OLAP?

**OLAP (Online Analytical Processing)** is a category of data processing optimized for complex analytical queries. Key operations:

| Operation | Description |
|-----------|-------------|
| **Roll-up** | Aggregate data by climbing up hierarchy (day → month → year) |
| **Drill-down** | Disaggregate data by going down hierarchy (year → month → day) |
| **Slice** | Select a single dimension value (e.g., year = 2020) |
| **Dice** | Select multiple dimension values (e.g., year IN (2020, 2021) AND genre = 'Action') |
| **Pivot** | Rotate data axes for different perspectives |

---

## OLAP Analyses in API

### 1. Roll-up Operations

| Endpoint | Operation | Description |
|----------|-----------|-------------|
| `/api/overview/movies-per-year` | Roll-up by year | Aggregates movie count from individual movies to yearly totals |
| `/api/genres/share-by-decade` | Roll-up by decade | Aggregates genre counts into 10-year periods |
| `/api/temporal/mpaa-trend` | Roll-up by decade | MPAA rating distribution aggregated by decade |
| `/api/ratings/platform-comparison` | Roll-up by year | Averages ratings across platforms per year |

### 2. Slice Operations

| Endpoint | Operation | Description |
|----------|-----------|-------------|
| `/api/ratings/distribution?source=imdb` | Slice by source | Filters to single rating platform |
| `/api/movies?year=2020` | Slice by year | Filters to single year |
| `/api/movies?genre_id=5` | Slice by genre | Filters to single genre |

### 3. Dice Operations

| Endpoint | Operation | Description |
|----------|-----------|-------------|
| `/api/overview/movies-per-year?start_year=2000&end_year=2020` | Dice by year range | Multi-value year filter |
| `/api/ratings/cult-classics?min_rating=8&max_votes=10000` | Dice by rating + votes | Multi-dimension filter |
| `/api/movies?year=2020&genre_id=1&min_rating=7` | Dice multi-dimension | Year + genre + rating filter |

### 4. Aggregation Queries

| Endpoint | Measures | Aggregation |
|----------|----------|-------------|
| `/api/genres/avg-rating` | imdb_rating | AVG grouped by genre |
| `/api/temporal/runtime-trend` | runtime | AVG grouped by year |
| `/api/temporal/quality-by-month` | imdb_rating | AVG grouped by month |
| `/api/people/top-actors` | movie count, avg_rating | COUNT, AVG grouped by person |
| `/api/people/top-directors` | movie count, avg_rating | COUNT, AVG grouped by person |

### 5. Ranking/Top-N Queries

| Endpoint | Ranking By |
|----------|------------|
| `/api/overview/top-popular` | imdb_votes, tmdb_popularity |
| `/api/genres/top-by-genre` | imdb_rating within genre |
| `/api/people/top-actors` | movie count or avg rating |

### 6. Distribution Analysis

| Endpoint | Analysis Type |
|----------|---------------|
| `/api/ratings/distribution` | Histogram binning |
| `/api/overview/language-distribution` | Categorical distribution |
| `/api/temporal/mpaa-distribution` | Categorical distribution |

### 7. Correlation Analysis

| Endpoint | Variables |
|----------|-----------|
| `/api/ratings/runtime-vs-rating` | Runtime × IMDb Rating scatter |

### 8. Network/Graph Analysis

| Endpoint | Analysis |
|----------|----------|
| `/api/people/collaboration-network` | Actor co-occurrence graph |

---

## Summary

The API implements **18+ OLAP operations** covering:
- ✅ Temporal analysis (year, decade, month trends)
- ✅ Categorical analysis (genre, MPAA, language)
- ✅ Multi-source comparison (IMDb, TMDB, MovieLens, RT)
- ✅ Entity analysis (actors, directors, movies)
- ✅ Network analysis (collaboration patterns)
