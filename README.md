# Big Movies

```
- Tích hợp và xử lý dữ liệu lớn
- Trực quan hóa dữ liệu
```

Big Movies is a full data engineering + analytics project that integrates 4 movie data sources into a unified **S3 data lake** (medallion architecture) and a **PostgreSQL (Supabase) star schema**, exposed via a **FastAPI** REST API and visualized in a **Next.js dashboard**.

## Data Sources

| Source              | Format                | Description                                                |
| ------------------- | --------------------- | ---------------------------------------------------------- |
| **MovieLens 32M**   | CSV                   | ~32 million ratings, 2 million tags, 87,585 movies         |
| **IMDb**            | TSV                   | Comprehensive metadata, director/cast info, global ratings |
| **Rotten Tomatoes** | Web Scraping → NDJSON | Tomatometer, audience scores, MPAA ratings, box office     |
| **TMDB**            | REST API → NDJSON     | Movie posters, plot summaries, popularity metrics          |

## Architecture

![Architecture](./movie-architecture.png)

The system follows a modern data engineering pipeline with medallion architecture:

1. **Ingestion Layer** — Downloads/scrapes from 4 data sources to S3 raw zone
2. **Bronze Layer** — Raw data parsed to Parquet with minimal transformation
3. **Silver Layer** — Cleaned, standardized data with entity resolution (unified movie IDs)
4. **Gold Layer** — Analytics-ready star schema (dimensions, facts, bridges)
5. **Data Warehouse** — PostgreSQL (Supabase) with star schema
6. **API Layer** — FastAPI REST service
7. **Frontend** — Next.js dashboard with Recharts visualizations

## Tech Stack

| Layer        | Technology                       |
| ------------ | -------------------------------- |
| Storage/Lake | AWS S3                           |
| Processing   | Apache Spark (AWS EMR)           |
| Warehouse    | PostgreSQL (Supabase)            |
| Backend      | FastAPI (Python)                 |
| Frontend     | Next.js 16 + React 19 + Recharts |
| Data Formats | CSV / TSV / NDJSON → Parquet     |

## Repository Structure

```
big-movies/
├── ingestion/                    # Data ingestion scripts
│   ├── movielens/               # Download MovieLens dataset → S3
│   ├── imdb/                    # Download IMDb TSV files → S3
│   ├── rotten_tomatoes/         # Web scraper → S3
│   └── tmdb/                    # TMDB API client → S3
│
├── processing/                   # Spark ETL (medallion architecture)
│   ├── jobs/
│   │   ├── bronze/              # Raw → Parquet ingestion
│   │   │   ├── ingest_movielens.py
│   │   │   ├── ingest_imdb.py
│   │   │   ├── ingest_tmdb.py
│   │   │   └── ingest_rotten_tomatoes.py
│   │   ├── silver/              # Cleaning & standardization
│   │   │   ├── build_entity_spine.py
│   │   │   ├── clean_movielens.py
│   │   │   ├── clean_imdb.py
│   │   │   ├── clean_tmdb.py
│   │   │   └── clean_rotten_tomatoes.py
│   │   └── gold/                # Star schema construction
│   │       ├── build_dim_*.py   # Dimension tables
│   │       ├── build_fact_metrics.py
│   │       ├── build_bridge_*.py
│   │       └── load_postgres.py
│   ├── lib/                     # Shared utilities
│   ├── config/                  # Spark configs & mappings
│   └── scripts/                 # Deployment scripts
│
├── warehouse/
│   └── schema.sql               # PostgreSQL star schema + RLS policies
│
├── backend/                      # FastAPI REST API
│   ├── app/
│   │   ├── routers/             # API endpoints
│   │   │   ├── overview.py
│   │   │   ├── ratings.py
│   │   │   ├── genres.py
│   │   │   ├── people.py
│   │   │   ├── movies.py
│   │   │   └── temporal.py
│   │   ├── database.py
│   │   └── config.py
│   └── main.py
│
├── frontend/                     # Next.js dashboard
│   ├── app/
│   │   ├── movies/
│   │   └── people/
│   ├── components/
│   └── lib/
│
├── data-lake/                    # Local data lake (dev)
└── docs/                         # Documentation
```

## Data Lake Layout (S3)

```
s3://movies-datalake-2310/
├── raw/                          # From ingestion scripts
│   ├── movielens/{date}/        # CSV files
│   ├── imdb/{date}/             # TSV files
│   ├── rotten_tomatoes/{date}/  # NDJSON files
│   └── tmdb/{date}/             # NDJSON files
├── bronze/                       # Parsed Parquet (minimal transform)
│   ├── movielens/
│   ├── imdb/
│   ├── rotten_tomatoes/
│   └── tmdb/
├── silver/                       # Cleaned & standardized
│   ├── entity_spine/            # Unified movie ID mapping
│   ├── movielens/
│   ├── imdb/
│   ├── rotten_tomatoes/
│   └── tmdb/
└── gold/                         # Star schema outputs
    ├── dim_movie/
    ├── dim_person/
    ├── dim_genre/
    ├── dim_time/
    ├── fact_movie_metrics/
    ├── bridge_movie_cast/
    └── bridge_movie_genres/
```

## Data Model (Star Schema)

### Dimension Tables

- `dim_time` — Date dimension (1950-2030)
- `dim_genre` — Canonical genre list
- `dim_person` — Actors, directors, crew from IMDb
- `dim_movie` — Unified movie dimension with cross-source IDs

### Fact Table

- `fact_movie_metrics` — All ratings and metrics aggregated (ML, IMDb, TMDB, RT, box office)

### Bridge Tables

- `bridge_movie_cast` — Movie ↔ Person relationships
- `bridge_movie_genres` — Movie ↔ Genre relationships

### Views

- `v_movie_full` — Complete movie info with all metrics
- `v_movie_genres` — Movies with aggregated genre names
- `v_movie_cast` — Movies with cast/crew details

See [`warehouse/schema.sql`](warehouse/schema.sql) for full schema definition.

## Key Features

- **Divisive Analysis** — Identify "controversial movies" by calculating critic vs. audience score differences
- **Multi-Platform Comparison** — Compare ratings across IMDb, TMDB, MovieLens, and Rotten Tomatoes
- **Genre Evolution** — Track genre popularity trends over decades
- **Cult Classics Discovery** — Find hidden gems with high ratings but low vote counts
- **Cast/Crew Networks** — Explore actor collaboration patterns

## Setup & Installation

### Prerequisites

- AWS account with S3 bucket + EMR cluster (Spark 3.5+, EMR 7.x)
- Supabase project (PostgreSQL)
- Python 3.10+ with `uv` package manager
- Node.js 20+ with `bun` or `npm`

### 1. Configure Environment Variables

Each service requires its own `.env` file:

**Ingestion (TMDB):**

```bash
cd ingestion/tmdb
cp env.example .env
# Set TMDB_API_KEY
```

**Processing:**

```bash
cd processing
cp .env.example .env
# Set AWS credentials, S3_BUCKET, DATABASE_URL
```

**Backend:**

```bash
cd backend
cp .env.example .env
# Set DATABASE_URL (Supabase connection string)
```

### 2. Run Ingestion Scripts

```bash
# MovieLens
cd ingestion/movielens && bun install && bun run index.ts

# IMDb
cd ingestion/imdb && bun install && bun run index.ts

# Rotten Tomatoes
cd ingestion/rotten_tomatoes && bun install && bun run index.ts

# TMDB
cd ingestion/tmdb && bun install && bun run index.ts
```

### 3. Run Spark ETL on EMR

Upload jobs and run in sequence:

```bash
cd processing

# Package shared libraries
zip -r lib.zip lib/

# Upload to S3
aws s3 sync jobs/ s3://$S3_BUCKET/processing/jobs/
aws s3 cp lib.zip s3://$S3_BUCKET/processing/

# Submit jobs (example)
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files s3://$S3_BUCKET/processing/lib.zip \
  s3://$S3_BUCKET/processing/jobs/bronze/ingest_movielens.py
```

**Execution order:**

1. Bronze jobs (parallel)
2. Silver: `build_entity_spine.py` first, then cleaners
3. Gold: dimensions → facts/bridges → `load_postgres.py`

See [`processing/SPARK_ETL_PLAN.md`](processing/SPARK_ETL_PLAN.md) for detailed execution plan.

### 4. Create Database Schema

Run in Supabase SQL Editor:

```sql
-- Execute contents of warehouse/schema.sql
```

### 5. Load Data to PostgreSQL

```bash
spark-submit ... s3://$S3_BUCKET/processing/jobs/gold/load_postgres.py
```

### 6. Start Backend API

```bash
cd backend
uv sync
uv run uvicorn main:app --reload
```

API available at: `http://localhost:8000`  
Swagger docs: `http://localhost:8000/docs`

### 7. Start Frontend Dashboard

```bash
cd frontend
bun install
bun run dev
```

Dashboard available at: `http://localhost:3000`

## API Endpoints

| Category | Endpoint                               | Description               |
| -------- | -------------------------------------- | ------------------------- |
| Overview | `GET /api/overview/movies-per-year`    | Movie count by year       |
| Overview | `GET /api/overview/top-popular`        | Top movies by popularity  |
| Ratings  | `GET /api/ratings/distribution`        | Rating histogram          |
| Ratings  | `GET /api/ratings/platform-comparison` | Cross-platform comparison |
| Ratings  | `GET /api/ratings/cult-classics`       | Hidden gems discovery     |
| Genres   | `GET /api/genres/share-by-decade`      | Genre trends over time    |
| Genres   | `GET /api/genres/avg-rating`           | Genre quality ranking     |
| People   | `GET /api/people/top-actors`           | Most prolific actors      |
| People   | `GET /api/people/{person_id}`          | Person filmography        |
| Movies   | `GET /api/movies`                      | Paginated movie list      |
| Movies   | `GET /api/movies/{movie_id}`           | Full movie details        |
| Temporal | `GET /api/temporal/runtime-trend`      | Runtime evolution         |

See [`backend/API_DOCS.md`](backend/API_DOCS.md) for complete API documentation.

## Orchestration (Optional)

For production scheduled runs, consider:

- **AWS Step Functions** — Native AWS orchestration
- **Apache Airflow** — Full-featured DAG orchestration

## Troubleshooting

| Issue                        | Solution                                                                         |
| ---------------------------- | -------------------------------------------------------------------------------- |
| Slow Spark jobs              | Increase `spark.sql.shuffle.partitions`, filter early, repartition before writes |
| Supabase connection fails    | Check IP allowlist, verify connection string, ensure SSL settings                |
| Missing joins across sources | Verify `entity_spine` outputs and ID normalization (`tt123...`, `nm123...`)      |
| Frontend can't reach API     | Check CORS settings, verify API base URL in frontend config                      |

## Disclaimers

- **Rotten Tomatoes** data is obtained via web scraping. Use responsibly and for educational purposes only.
- **MovieLens** dataset is provided for research purposes under their [terms of use](https://grouplens.org/datasets/movielens/).
- **IMDb** datasets are available for personal/non-commercial use under their [terms](https://www.imdb.com/interfaces/).
- **TMDB** API usage requires attribution per their [terms of service](https://www.themoviedb.org/documentation/api/terms-of-use).

## License

This project is for educational and demonstration purposes.
