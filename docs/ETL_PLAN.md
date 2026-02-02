# Big Movies ETL Implementation Plan

This document outlines the ETL pipeline design for transforming raw data from 4 heterogeneous sources into a unified star schema data warehouse.

## 1. Data Sources Overview

| Source              | Format | Method            | Key Characteristics                                      |
| ------------------- | ------ | ----------------- | -------------------------------------------------------- |
| **MovieLens**       | CSV    | Periodic Download | 32M ratings, pipe-separated genres, well-structured      |
| **IMDb**            | TSV    | Daily Download    | `\N` for nulls, comma-separated genres, large volume     |
| **Rotten Tomatoes** | NDJSON | Web Scraping      | Nullable fields, `$100M` box office format, scraped data |
| **TMDB**            | NDJSON | REST API          | `genre_ids` as int array, relative poster paths          |

---

## 2. Data Heterogeneity Analysis

### 2.1 Format & Null Handling

| Issue                   | MovieLens    | IMDb | Rotten Tomatoes | TMDB        |
| ----------------------- | ------------ | ---- | --------------- | ----------- |
| **File Format**         | CSV          | TSV  | NDJSON          | NDJSON      |
| **Null Representation** | Empty string | `\N` | JSON `null`     | JSON `null` |
| **Delimiter**           | Comma        | Tab  | N/A             | N/A         |

### 2.2 ID Systems

| Source          | Primary ID | Format             | Example     |
| --------------- | ---------- | ------------------ | ----------- |
| MovieLens       | `movieId`  | Integer            | `1`         |
| IMDb            | `tconst`   | String with prefix | `tt0114709` |
| Rotten Tomatoes | `rt_id`    | URL slug           | `toy_story` |
| TMDB            | `id`       | Integer            | `862`       |

**Entity Resolution**: MovieLens `links.csv` provides the mapping spine:

- `movieId` (MovieLens) → `imdbId` (needs `tt` prefix) → `tmdbId` (TMDB)

### 2.3 Genre Representation

| Source          | Format                 | Example                                           |
| --------------- | ---------------------- | ------------------------------------------------- |
| MovieLens       | Pipe-separated string  | `Adventure\|Animation\|Children\|Comedy\|Fantasy` |
| IMDb            | Comma-separated string | `Animation,Comedy,Romance`                        |
| Rotten Tomatoes | Comma-separated text   | `Kids & Family, Comedy, Adventure, Animation`     |
| TMDB            | Integer array          | `[10751, 14, 16, 10749]`                          |

### 2.4 Data Quality Issues Identified

1. **Missing Linkage**: RT data has no IMDb/TMDB IDs - requires title+year matching
2. **Box Office Parsing**: String formats like `$100M`, `$1.2B`, `$950K`, `$12,345,678`
3. **Nullable Scores**: Many RT records lack `audience_score` (visible in sample data)
4. **Multiple Directors**: RT stores as comma-separated string (`"Jared Bush, Byron Howard"`)
5. **Year Extraction**: MovieLens titles include year in parentheses (`"Toy Story (1995)"`)
6. **Poster URLs**: TMDB provides relative paths, need base URL prefix

---

## 3. Target Star Schema

### 3.1 Dimension Tables

#### `dim_movie`

| Column         | Type               | Source Priority                         |
| -------------- | ------------------ | --------------------------------------- |
| `movie_id`     | BIGINT (surrogate) | Generated (hash of best ID)             |
| `imdb_id`      | VARCHAR            | links.csv → IMDb                        |
| `tmdb_id`      | INTEGER            | links.csv → TMDB                        |
| `ml_id`        | INTEGER            | MovieLens movieId                       |
| `title`        | VARCHAR            | IMDb `primaryTitle` > MovieLens         |
| `year`         | INTEGER            | IMDb `startYear` > parsed from ML title |
| `runtime`      | INTEGER            | IMDb `runtimeMinutes`                   |
| `mpaa_rating`  | VARCHAR            | Rotten Tomatoes                         |
| `plot_summary` | TEXT               | TMDB `overview`                         |
| `poster_url`   | VARCHAR            | TMDB (prefixed)                         |

#### `dim_person`

| Column               | Type    | Source                  |
| -------------------- | ------- | ----------------------- |
| `person_id`          | VARCHAR | IMDb `nconst`           |
| `name`               | VARCHAR | IMDb `primaryName`      |
| `birth_year`         | INTEGER | IMDb `birthYear`        |
| `death_year`         | INTEGER | IMDb `deathYear`        |
| `primary_profession` | VARCHAR | IMDb (first profession) |

#### `dim_genre`

| Column       | Type    | Description          |
| ------------ | ------- | -------------------- |
| `genre_id`   | INTEGER | Surrogate key        |
| `genre_name` | VARCHAR | Canonical genre name |

#### `dim_time`

| Column        | Type    | Description     |
| ------------- | ------- | --------------- |
| `time_id`     | INTEGER | YYYYMMDD format |
| `date`        | DATE    | Full date       |
| `year`        | INTEGER | Year            |
| `quarter`     | INTEGER | 1-4             |
| `month`       | INTEGER | 1-12            |
| `day_of_week` | INTEGER | 1-7             |

### 3.2 Fact Table

#### `fact_movie_metrics`

| Column               | Type         | Source                        |
| -------------------- | ------------ | ----------------------------- |
| `metric_id`          | BIGINT       | Hash of (movie_id, time_id)   |
| `movie_id`           | BIGINT       | FK to dim_movie               |
| `time_id`            | INTEGER      | FK to dim_time                |
| `ml_avg_rating`      | DECIMAL(3,2) | MovieLens avg(rating)         |
| `ml_rating_count`    | INTEGER      | MovieLens count(\*)           |
| `imdb_rating`        | DECIMAL(3,1) | IMDb `averageRating`          |
| `imdb_votes`         | INTEGER      | IMDb `numVotes`               |
| `tomatometer_score`  | INTEGER      | RT critics score (0-100)      |
| `audience_score`     | INTEGER      | RT audience score (0-100)     |
| `divisive_score`     | INTEGER      | `abs(tomatometer - audience)` |
| `box_office_revenue` | BIGINT       | RT parsed to cents            |

### 3.3 Bridge Tables

#### `bridge_movie_cast`

| Column       | Type    | Source                       |
| ------------ | ------- | ---------------------------- |
| `movie_id`   | BIGINT  | FK to dim_movie              |
| `person_id`  | VARCHAR | FK to dim_person             |
| `category`   | VARCHAR | IMDb (actor, director, etc.) |
| `ordering`   | INTEGER | IMDb billing order           |
| `characters` | VARCHAR | IMDb character names         |

#### `bridge_movie_genres`

| Column     | Type    | Source          |
| ---------- | ------- | --------------- |
| `movie_id` | BIGINT  | FK to dim_movie |
| `genre_id` | INTEGER | FK to dim_genre |

---

## 4. ETL Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Data Sources                                      │
├─────────────┬─────────────┬─────────────────────┬───────────────────────┤
│  MovieLens  │    IMDb     │   Rotten Tomatoes   │       TMDB            │
│  (CSV)      │   (TSV)     │    (NDJSON)         │    (NDJSON)           │
└──────┬──────┴──────┬──────┴──────────┬──────────┴─────────┬─────────────┘
       │             │                 │                    │
       ▼             ▼                 ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   S3 Bronze Layer (Parsed Parquet)                       │
│  • Minimal transformation, add ingest_date                               │
│  • Replace \N → null (IMDb)                                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   S3 Silver Layer (Cleaned & Standardized)               │
│  • Normalize IDs (imdbId → tt0114709)                                    │
│  • Parse box office ($100M → 100000000)                                  │
│  • Split genres to arrays                                                │
│  • Build Entity Resolution Spine from links.csv                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   S3 Gold Layer (Star Schema)                            │
│  • dim_movie, dim_person, dim_genre, dim_time                            │
│  • fact_movie_metrics                                                    │
│  • bridge_movie_cast, bridge_movie_genres                                │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   PostgreSQL Data Warehouse                              │
│  • JDBC load via staging tables                                          │
│  • Truncate + insert (full refresh)                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Spark Jobs Structure

```
processing/
├── jobs/
│   ├── 00_ingest_movielens.py      # CSV → Bronze Parquet
│   ├── 01_ingest_imdb.py           # TSV → Bronze (handle \N nulls)
│   ├── 02_ingest_tmdb.py           # NDJSON → Bronze
│   ├── 03_ingest_rotten_tomatoes.py # NDJSON → Bronze
│   ├── 10_build_entity_spine.py    # Entity resolution using links.csv
│   ├── 20_build_dimensions.py      # Build all dimension tables
│   ├── 30_build_facts.py           # Build fact + bridge tables
│   └── 40_load_postgres.py         # Load to PostgreSQL via JDBC
├── lib/
│   ├── io.py                       # S3 readers/writers, schema definitions
│   ├── cleaning.py                 # Null normalization, parsers
│   ├── keys.py                     # ID normalization, surrogate key generation
│   └── dq.py                       # Data quality validations
└── config/
    └── genre_mapping.csv           # Canonical genre dictionary
```

---

## 6. Critical Transformations

### 6.1 IMDb ID Normalization

```python
# MovieLens imdbId (without prefix) → IMDb tconst (with prefix)
imdb_id = 'tt' + str(imdb_id).zfill(7)
# Example: 114709 → tt0114709
```

### 6.2 Box Office Parsing

```python
def parse_box_office(value: str) -> int:
    """Parse $100M, $1.2B, $950K formats to cents"""
    if not value:
        return None
    value = value.upper().replace('$', '').replace(',', '').strip()
    multipliers = {'K': 1_000, 'M': 1_000_000, 'B': 1_000_000_000}
    for suffix, mult in multipliers.items():
        if value.endswith(suffix):
            return int(float(value[:-1]) * mult)
    return int(float(value))
```

### 6.3 Divisive Score Calculation

```python
divisive_score = abs(tomatometer_score - audience_score) if both_present else None
```

### 6.4 TMDB Poster URL Construction

```python
poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
```

### 6.5 MovieLens Title Year Extraction

```python
import re
match = re.search(r'\((\d{4})\)\s*$', title)
year = int(match.group(1)) if match else None
clean_title = re.sub(r'\s*\(\d{4}\)\s*$', '', title)
```

---

## 7. Entity Resolution Strategy

### Primary Path (High Confidence)

1. Start from MovieLens `links.csv` as the backbone
2. `ml_id` → `imdb_id` → `tmdb_id` mapping is authoritative
3. Join IMDb data via `tconst = imdb_id`
4. Join TMDB data via `tmdb_id`

### Fallback Path (For Rotten Tomatoes)

1. If RT record has IMDb ID → direct join
2. Else: match on `normalized_title + year` with strict rules:
   - Exact year match (±1 year tolerance optional)
   - Normalized title match (lowercase, no punctuation)
3. Track `match_method` and `match_confidence`

### Title Normalization Function

```python
def normalize_title(title: str) -> str:
    title = title.lower().strip()
    title = re.sub(r'[^\w\s]', '', title)  # Remove punctuation
    title = re.sub(r'\s+', ' ', title)      # Collapse whitespace
    title = re.sub(r'\s*\(\d{4}\)\s*$', '', title)  # Remove year suffix
    return title
```

---

## 8. Genre Standardization

### Canonical Genre Mapping (Example)

| Source    | Source Genre  | Canonical Genre |
| --------- | ------------- | --------------- |
| MovieLens | Sci-Fi        | Science Fiction |
| IMDb      | Sci-Fi        | Science Fiction |
| TMDB      | 878           | Science Fiction |
| RT        | Sci-Fi        | Science Fiction |
| MovieLens | Children      | Family          |
| TMDB      | 10751         | Family          |
| RT        | Kids & Family | Family          |

### Unmapped Genres

- Log to `unmapped_genres` table for review
- Default to `Other` category

---

## 9. Data Quality Checks

| Check        | Table              | Rule                                      |
| ------------ | ------------------ | ----------------------------------------- |
| Uniqueness   | dim_movie          | `movie_id` unique                         |
| Uniqueness   | dim_person         | `person_id` unique                        |
| Referential  | fact_movie_metrics | `movie_id` exists in dim_movie            |
| Referential  | bridge_movie_cast  | `movie_id`, `person_id` exist             |
| Range        | fact_movie_metrics | `tomatometer_score` 0-100                 |
| Range        | fact_movie_metrics | `audience_score` 0-100                    |
| Non-negative | fact_movie_metrics | `box_office_revenue >= 0`                 |
| Not null     | dim_movie          | At least one of (imdb_id, tmdb_id, ml_id) |

---

## 10. Implementation Phases

### Phase 1: Bronze Layer (Ingestion)

- [x] Implement Spark readers for each source format
- [x] Handle null representations per source
- [x] Write to Parquet with `ingest_date` partition

### Phase 2: Silver Layer (Cleaning)

- [x] Normalize IDs across all sources
- [x] Parse and validate box office values
- [x] Build entity resolution spine from links.csv
- [x] Standardize genre mappings

### Phase 3: Gold Layer (Star Schema)

- [x] Build dimension tables with proper sourcing priority
- [x] Aggregate MovieLens ratings for fact table
- [x] Calculate derived metrics (divisive_score)
- [x] Build bridge tables with proper FK relationships

### Phase 4: PostgreSQL Load

- [x] Create staging tables in PostgreSQL
- [x] Implement JDBC batch loading
- [x] Add upsert/truncate-insert logic
- [ ] Validate row counts and referential integrity

---

## 11. Risks & Mitigations

| Risk                      | Impact                  | Mitigation                                      |
| ------------------------- | ----------------------- | ----------------------------------------------- |
| RT records without IDs    | Data loss               | Title+year fallback matching with logging       |
| Box office parse failures | Null values             | Keep raw value + parse_success flag             |
| Genre mapping gaps        | Inconsistent categories | Default to "Other" + log for review             |
| IMDb data volume          | Slow processing         | Project only needed columns, partition wisely   |
| Duplicate entities        | Data quality            | Deduplicate by best ID with deterministic logic |

---

## 12. Sample Data Observations

### MovieLens movies.csv

```csv
movieId,title,genres
1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
```

### IMDb title.basics.tsv

```tsv
tconst  titleType  primaryTitle  startYear  runtimeMinutes  genres
tt0114709  movie  Toy Story  1995  81  Adventure,Animation,Comedy,Family,Fantasy
```

### Rotten Tomatoes NDJSON

```json
{
  "rt_id": "zootopia_2",
  "title": "Zootopia 2",
  "tomatometer_score": 91,
  "audience_score": null,
  "mpaa_rating": "PG",
  "box_office": null,
  "director": "Jared Bush, Byron Howard"
}
```

### TMDB NDJSON

```json
{
  "id": 11224,
  "title": "Cinderella",
  "genre_ids": [10751, 14, 16, 10749],
  "poster_path": "/4nssBcQUBadCTBjrAkX46mVEKts.jpg",
  "vote_average": 7.045
}
```

---

_Generated: 2026-01-31_
