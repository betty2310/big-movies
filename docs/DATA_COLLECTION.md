# Big Movies Data Collection

This document describes the data collected in the Big Movies project, including data sources, schemas, field definitions, and collection methods.

## Overview

The project integrates data from **4 distinct sources** to build a comprehensive movie analytics platform. Each source compensates for limitations in others (e.g., MovieLens lacks crew/financial data, IMDb lacks audience scores).

| Source | Type | Method | Key Information |
|--------|------|--------|-----------------|
| **MovieLens** | Structured (CSV) | Periodic Download | 32M ratings, 2M tags, 87k movies |
| **IMDb** | Structured (TSV) | Daily Download | Metadata, global ratings, crew/cast |
| **Rotten Tomatoes** | Unstructured (HTML) | Web Scraping | Critic/Audience scores, Box Office |
| **TMDB** | Semi-structured (JSON) | REST API | Movie posters, plot summaries |

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Data Sources                                  │
├─────────────┬─────────────┬─────────────────────┬───────────────────┤
│  MovieLens  │    IMDb     │   Rotten Tomatoes   │       TMDB        │
│  (ZIP/CSV)  │  (GZ/TSV)   │    (HTML Scrape)    │    (REST API)     │
└──────┬──────┴──────┬──────┴──────────┬──────────┴─────────┬─────────┘
       │             │                 │                    │
       ▼             ▼                 ▼                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   S3 Raw Zone (s3://movies-datalake-2310/raw/)      │
│  └── movielens/{date}/   imdb/{date}/   rotten_tomatoes/{date}/     │
│                          tmdb/{date}/                               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 1. MovieLens

### Source
- **URL**: `https://files.grouplens.org/datasets/movielens/ml-32m.zip`
- **Format**: ZIP archive containing CSV files
- **Collection Method**: Periodic download with checksum comparison

### Files Collected

| File | Description |
|------|-------------|
| `ratings.csv` | User ratings (32M+ entries) |
| `movies.csv` | Movie metadata (87k movies) |
| `tags.csv` | User-generated tags (2M+ tags) |
| `links.csv` | External ID mappings (IMDb, TMDB) |

### Schema: `ratings.csv`

| Field | Type | Description |
|-------|------|-------------|
| `userId` | Integer | Unique user identifier |
| `movieId` | Integer | MovieLens movie ID |
| `rating` | Float | Rating on 0.5-5.0 scale (half-star increments) |
| `timestamp` | Integer | Unix timestamp of rating |

### Schema: `movies.csv`

| Field | Type | Description |
|-------|------|-------------|
| `movieId` | Integer | Primary key for MovieLens |
| `title` | String | Movie title with year (e.g., "Toy Story (1995)") |
| `genres` | String | Pipe-separated genres (e.g., "Animation\|Comedy\|Family") |

### Schema: `tags.csv`

| Field | Type | Description |
|-------|------|-------------|
| `userId` | Integer | User who applied the tag |
| `movieId` | Integer | MovieLens movie ID |
| `tag` | String | Free-form text tag |
| `timestamp` | Integer | Unix timestamp |

### Schema: `links.csv`

| Field | Type | Description |
|-------|------|-------------|
| `movieId` | Integer | MovieLens movie ID |
| `imdbId` | String | IMDb ID (without "tt" prefix) |
| `tmdbId` | Integer | TMDB movie ID |

### How We Get It
```bash
cd ingestion/movielens
bun install
bun run index.ts
```
- Downloads ZIP from GroupLens
- Computes MD5 checksum to detect changes
- Extracts and uploads CSVs to `s3://movies-datalake-2310/raw/movielens/{date}/`

---

## 2. IMDb

### Source
- **Base URL**: `https://datasets.imdbws.com`
- **Format**: Gzipped TSV files (tab-separated)
- **Collection Method**: Daily download of public datasets

### Files Collected

| File | Description |
|------|-------------|
| `name.basics.tsv` | Person information (actors, directors, etc.) |
| `title.akas.tsv` | Alternative titles by region |
| `title.basics.tsv` | Core movie metadata |
| `title.crew.tsv` | Director and writer associations |
| `title.episode.tsv` | TV episode information |
| `title.principals.tsv` | Principal cast/crew for titles |
| `title.ratings.tsv` | IMDb ratings and vote counts |

### Schema: `title.basics.tsv`

| Field | Type | Description |
|-------|------|-------------|
| `tconst` | String | Unique title ID (e.g., "tt0000001") |
| `titleType` | String | Type: movie, short, tvSeries, etc. |
| `primaryTitle` | String | Display title |
| `originalTitle` | String | Original language title |
| `isAdult` | Boolean | Adult content flag (0/1) |
| `startYear` | Integer | Release year |
| `endYear` | Integer | End year (for TV series) |
| `runtimeMinutes` | Integer | Runtime in minutes |
| `genres` | String | Comma-separated genres |

### Schema: `title.ratings.tsv`

| Field | Type | Description |
|-------|------|-------------|
| `tconst` | String | Title ID |
| `averageRating` | Float | Weighted average rating (0-10) |
| `numVotes` | Integer | Total number of votes |

### Schema: `name.basics.tsv`

| Field | Type | Description |
|-------|------|-------------|
| `nconst` | String | Person ID (e.g., "nm0000001") |
| `primaryName` | String | Name as credited |
| `birthYear` | Integer | Year of birth |
| `deathYear` | Integer | Year of death (if applicable) |
| `primaryProfession` | String | Comma-separated professions |
| `knownForTitles` | String | Comma-separated title IDs |

### Schema: `title.principals.tsv`

| Field | Type | Description |
|-------|------|-------------|
| `tconst` | String | Title ID |
| `ordering` | Integer | Billing order (1 = top billed) |
| `nconst` | String | Person ID |
| `category` | String | Role type: actor, actress, director, etc. |
| `job` | String | Specific job (for crew) |
| `characters` | String | Character names played (JSON array) |

### Schema: `title.crew.tsv`

| Field | Type | Description |
|-------|------|-------------|
| `tconst` | String | Title ID |
| `directors` | String | Comma-separated director person IDs |
| `writers` | String | Comma-separated writer person IDs |

### How We Get It
```bash
cd ingestion/imdb
bun install
bun run index.ts
```
- Downloads all 7 datasets from IMDb
- Computes combined MD5 checksum
- Extracts .gz files and uploads TSVs to `s3://movies-datalake-2310/raw/imdb/{date}/`

---

## 3. Rotten Tomatoes

### Source
- **Base URL**: `https://www.rottentomatoes.com`
- **Format**: HTML pages (unstructured)
- **Collection Method**: Web scraping with rate limiting

### Collection Strategy
1. **Phase 1 - Discovery**: Crawl browse pages to discover movie slugs
   - Browse categories: movies at home, in theaters, coming soon
   - Filters: by genre (18 genres), by critic score (certified fresh, fresh, rotten), by audience score
   - Sort options: newest, popular, A-Z
2. **Phase 2 - Scraping**: Fetch each movie detail page
3. **Phase 3 - Upload**: Upload as NDJSON chunks to S3

### Schema: `MovieRecord` (output)

| Field | Type | Description |
|-------|------|-------------|
| `rt_id` | String | Rotten Tomatoes identifier (slug) |
| `slug` | String | URL slug (e.g., "the_godfather") |
| `title` | String | Movie title |
| `tomatometer_score` | Integer \| null | Critics score (0-100%) |
| `audience_score` | Integer \| null | Audience score (0-100%) |
| `mpaa_rating` | String \| null | MPAA rating (G, PG, PG-13, R, NC-17) |
| `box_office` | String \| null | Box office revenue (e.g., "$100M") |
| `release_date` | String \| null | Theatrical release date |
| `release_year` | Integer \| null | Year extracted from release date |
| `director` | String \| null | Director name(s) |
| `genre` | String \| null | Primary genre(s) |
| `scraped_at` | String | ISO timestamp of scraping |

### How We Get It
```bash
cd ingestion/rotten_tomatoes
bun install
bun run index.ts
```
- Uses rotating user agents to avoid detection
- Implements exponential backoff on rate limits (429/403)
- Maintains SQLite state for resumable crawling
- Filters movies before 1950
- Uploads in 1000-movie NDJSON chunks to `s3://movies-datalake-2310/raw/rotten_tomatoes/{date}/`

---

## 4. TMDB (The Movie Database)

### Source
- **Base URL**: `https://api.themoviedb.org/3`
- **Format**: JSON API responses
- **Collection Method**: REST API with bearer token authentication

### API Endpoint Used
```
GET /discover/movie
  ?primary_release_year={year}
  &sort_by=popularity.desc
  &page={page}
  &include_adult=false
```

### Schema: TMDB Movie Response

| Field | Type | Description |
|-------|------|-------------|
| `id` | Integer | TMDB movie ID |
| `title` | String | Movie title |
| `original_title` | String | Original language title |
| `overview` | String | Plot summary |
| `poster_path` | String | Poster image path (append to image base URL) |
| `backdrop_path` | String | Backdrop image path |
| `release_date` | String | Release date (YYYY-MM-DD) |
| `popularity` | Float | Popularity score |
| `vote_average` | Float | Average user rating (0-10) |
| `vote_count` | Integer | Number of votes |
| `genre_ids` | Array | Array of genre ID integers |
| `original_language` | String | ISO 639-1 language code |
| `adult` | Boolean | Adult content flag |
| `video` | Boolean | Has video flag |

### How We Get It
```bash
cd ingestion/tmdb
# Set API token in .env (see env.example)
echo "API_READ_ACCESS_TOKEN=your_token" > .env
bun install
bun run index.ts
```
- Requires TMDB API Read Access Token (v4 auth)
- Iterates through years 1950-present
- Paginates through up to 500 pages per year (API limit)
- Deduplicates by movie ID
- Implements state file for resumable ingestion
- Uploads in 1000-movie NDJSON chunks to `s3://movies-datalake-2310/raw/tmdb/{date}/`

---

## Destination: S3 Data Lake

All raw data is stored in AWS S3 with the following structure:

```
s3://movies-datalake-2310/
└── raw/
    ├── movielens/{date}/
    │   ├── ratings.csv
    │   ├── movies.csv
    │   ├── tags.csv
    │   └── links.csv
    ├── imdb/{date}/
    │   ├── name.basics.tsv
    │   ├── title.akas.tsv
    │   ├── title.basics.tsv
    │   ├── title.crew.tsv
    │   ├── title.episode.tsv
    │   ├── title.principals.tsv
    │   └── title.ratings.tsv
    ├── rotten_tomatoes/{date}/
    │   ├── movies_0001.ndjson
    │   ├── movies_0002.ndjson
    │   ├── ...
    │   └── metadata.json
    └── tmdb/{date}/
        ├── movies_0001.ndjson
        ├── movies_0002.ndjson
        ├── ...
        └── metadata.json
```

---

## Mediated Schema (After Processing)

After Spark processing, data is unified into a **Star Schema** in PostgreSQL:

### Dimension Tables

| Table | Key Fields | Description |
|-------|------------|-------------|
| `dim_movie` | movie_id, title, year, runtime, mpaa_rating, plot_summary, poster_url | Core movie metadata |
| `dim_person` | person_id, name, birth_year, death_year | Actors, directors |
| `dim_genre` | genre_id, genre_name | Standardized genres |
| `dim_time` | date_key, year, month, quarter | Time dimension |

### Fact Table

| Table | Metrics | Description |
|-------|---------|-------------|
| `fact_movie_metrics` | ml_avg_rating, ml_rating_count, imdb_rating, imdb_votes, tomatometer, audience_score, divisive_score, box_office | All quantitative metrics |

### Bridge Tables

| Table | Purpose |
|-------|---------|
| `bridge_movie_cast` | Many-to-many: movies ↔ persons with role and ordering |
| `bridge_movie_genres` | Many-to-many: movies ↔ genres |

---

## Key Metrics

| Metric | Source | Description |
|--------|--------|-------------|
| `ml_avg_rating` | MovieLens | Average user rating (0.5-5.0 scale) |
| `ml_rating_count` | MovieLens | Number of MovieLens ratings |
| `imdb_rating` | IMDb | Weighted average (0-10 scale) |
| `imdb_votes` | IMDb | Total vote count |
| `tomatometer` | Rotten Tomatoes | Critics score (0-100%) |
| `audience_score` | Rotten Tomatoes | Audience score (0-100%) |
| `divisive_score` | Calculated | abs(tomatometer - audience_score) |
| `box_office` | Rotten Tomatoes | Revenue in USD |
