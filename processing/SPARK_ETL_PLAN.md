# Spark ETL Processing Plan for AWS EMR

## Cluster Configuration

| Component | Specification |
|-----------|---------------|
| **EMR Version** | emr-7.x (Spark 3.5+) |
| **Primary Node** | 1x EC2 (coordinator) |
| **Core Node** | 1x EC2 (executor) |
| **S3 Bucket** | `s3://movies-datalake-2310/` |

---

## Project Structure

```
processing/
├── jobs/                           # Spark jobs (submitted to EMR)
│   ├── bronze/
│   │   ├── ingest_movielens.py     # CSV → Bronze Parquet
│   │   ├── ingest_imdb.py          # TSV → Bronze Parquet
│   │   ├── ingest_tmdb.py          # NDJSON → Bronze Parquet
│   │   └── ingest_rotten_tomatoes.py
│   ├── silver/
│   │   ├── build_entity_spine.py   # Entity resolution (links.csv)
│   │   ├── clean_movielens.py      # Standardize ML data
│   │   ├── clean_imdb.py           # Handle \N nulls, normalize IDs
│   │   ├── clean_tmdb.py           # Build poster URLs
│   │   └── clean_rotten_tomatoes.py # Parse box office
│   └── gold/
│       ├── build_dim_movie.py      # Dimension: movies
│       ├── build_dim_person.py     # Dimension: cast/crew
│       ├── build_dim_genre.py      # Dimension: genres
│       ├── build_dim_time.py       # Dimension: time
│       ├── build_fact_metrics.py   # Fact: movie metrics
│       ├── build_bridge_cast.py    # Bridge: movie-person
│       ├── build_bridge_genres.py  # Bridge: movie-genre
│       └── load_postgres.py        # JDBC load to PostgreSQL
├── lib/                            # Shared utilities
│   ├── __init__.py
│   ├── io_utils.py                 # S3 read/write helpers
│   ├── transformations.py          # Common transformations
│   ├── entity_resolution.py        # Title matching, ID normalization
│   └── data_quality.py             # DQ checks
├── config/
│   ├── paths.py                    # S3 path constants
│   ├── schemas.py                  # Spark schema definitions
│   └── genre_mapping.csv           # Canonical genre dictionary
├── scripts/
│   ├── deploy.sh                   # Upload to S3 & submit to EMR
│   ├── run_bronze.sh               # Run all bronze jobs
│   ├── run_silver.sh               # Run all silver jobs
│   └── run_gold.sh                 # Run all gold jobs
└── requirements.txt                # Python dependencies
```

---

## S3 Data Lake Structure

```
s3://movies-datalake-2310/
├── raw/                            # From ingestion scripts (already exists)
│   ├── movielens/{date}/           # CSV files
│   ├── imdb/{date}/                # TSV files
│   ├── rotten_tomatoes/{date}/     # NDJSON files
│   └── tmdb/{date}/                # NDJSON files
├── bronze/                         # Parsed to Parquet (minimal transform)
│   ├── movielens/
│   ├── imdb/
│   ├── rotten_tomatoes/
│   └── tmdb/
├── silver/                         # Cleaned & standardized
│   ├── entity_spine/               # movie_id mapping table
│   ├── movielens/
│   ├── imdb/
│   ├── rotten_tomatoes/
│   └── tmdb/
├── gold/                           # Star schema (final)
│   ├── dim_movie/
│   ├── dim_person/
│   ├── dim_genre/
│   ├── dim_time/
│   ├── fact_movie_metrics/
│   ├── bridge_movie_cast/
│   └── bridge_movie_genres/
└── processing/                     # Job artifacts
    ├── jobs/                       # Uploaded Python scripts
    └── lib/                        # Shared libraries
```

---

## Execution Plan

### Phase 1: Bronze Layer (Raw → Parquet)

| Job | Input | Output | Key Logic |
|-----|-------|--------|-----------|
| `ingest_movielens.py` | `raw/movielens/` CSV | `bronze/movielens/` | Add `ingest_date`, preserve schema |
| `ingest_imdb.py` | `raw/imdb/` TSV | `bronze/imdb/` | Replace `\N` → null |
| `ingest_tmdb.py` | `raw/tmdb/` NDJSON | `bronze/tmdb/` | Flatten JSON, add `ingest_date` |
| `ingest_rotten_tomatoes.py` | `raw/rotten_tomatoes/` NDJSON | `bronze/rotten_tomatoes/` | Preserve nulls |

**EMR Submit Example:**
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files s3://movies-datalake-2310/processing/lib.zip \
  s3://movies-datalake-2310/processing/jobs/bronze/ingest_movielens.py \
  --input-date 2026-01-31
```

### Phase 2: Silver Layer (Clean & Standardize)

| Job | Key Transformations |
|-----|---------------------|
| `build_entity_spine.py` | Create unified `movie_id` from links.csv: `ml_id → imdb_id → tmdb_id` |
| `clean_movielens.py` | Extract year from title, split genres, compute avg ratings |
| `clean_imdb.py` | Normalize `tconst` format, filter `titleType='movie'` |
| `clean_tmdb.py` | Build full poster URLs, map `genre_ids` to names |
| `clean_rotten_tomatoes.py` | Parse box office (`$100M` → `100000000`), title normalization |

**Entity Spine Logic:**
```
┌─────────────────────────────────────────────────────────┐
│                   Entity Spine                          │
├─────────────────────────────────────────────────────────┤
│  movie_id (surrogate) │ ml_id │ imdb_id │ tmdb_id │ rt_slug │
│  hash(imdb_id)        │  1    │tt0114709│  862    │toy_story│
└─────────────────────────────────────────────────────────┘
```

### Phase 3: Gold Layer (Star Schema)

| Job | Dependencies | Output |
|-----|--------------|--------|
| `build_dim_time.py` | None | Generate date dimension (1950-2030) |
| `build_dim_genre.py` | genre_mapping.csv | Canonical genre list |
| `build_dim_person.py` | silver/imdb/name.basics | Person dimension |
| `build_dim_movie.py` | Entity spine + all silver sources | Movie dimension |
| `build_fact_metrics.py` | dim_movie + silver sources | Aggregated metrics + divisive_score |
| `build_bridge_cast.py` | dim_movie + silver/imdb/principals | Movie-Person relationships |
| `build_bridge_genres.py` | dim_movie + dim_genre | Movie-Genre relationships |
| `load_postgres.py` | All gold tables | JDBC load to PostgreSQL |

---

## Deployment Workflow

### Step 1: Package & Upload
```bash
# Package shared libraries
cd processing
zip -r lib.zip lib/

# Upload to S3
aws s3 sync jobs/ s3://movies-datalake-2310/processing/jobs/
aws s3 cp lib.zip s3://movies-datalake-2310/processing/
aws s3 cp config/ s3://movies-datalake-2310/processing/config/ --recursive
```

### Step 2: Submit Jobs (Sequential)
```bash
# Bronze (can run in parallel)
aws emr add-steps --cluster-id j-XXXXX --steps '[
  {"Name":"Bronze-MovieLens","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","--py-files","s3://movies-datalake-2310/processing/lib.zip",
   "s3://movies-datalake-2310/processing/jobs/bronze/ingest_movielens.py"]},
  {"Name":"Bronze-IMDb","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","--py-files","s3://movies-datalake-2310/processing/lib.zip",
   "s3://movies-datalake-2310/processing/jobs/bronze/ingest_imdb.py"]}
]'

# Silver (sequential - entity spine first)
aws emr add-steps --cluster-id j-XXXXX --steps '[
  {"Name":"Silver-EntitySpine","ActionOnFailure":"CANCEL_AND_WAIT","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","--py-files","s3://movies-datalake-2310/processing/lib.zip",
   "s3://movies-datalake-2310/processing/jobs/silver/build_entity_spine.py"]}
]'

# Gold (dimensions first, then facts)
# ... similar pattern
```

### Step 3: Orchestration (Optional)
For production, use **AWS Step Functions** or **Apache Airflow** to orchestrate job dependencies.

---

## Job Dependencies DAG

```
                    ┌──────────────────┐
                    │   Raw Data (S3)  │
                    └────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
┌───────────────┐   ┌───────────────┐   ┌─────────────────┐
│Bronze-MovieLens│   │ Bronze-IMDb  │   │Bronze-RT/TMDB   │
└───────┬───────┘   └───────┬───────┘   └────────┬────────┘
        │                   │                    │
        └───────────────────┼────────────────────┘
                            ▼
                 ┌──────────────────────┐
                 │  Silver-EntitySpine  │ (links.csv mapping)
                 └──────────┬───────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│Silver-MovieLens│   │  Silver-IMDb │   │Silver-RT/TMDB │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  dim_genre    │   │  dim_person   │   │   dim_time    │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            ▼
                    ┌───────────────┐
                    │   dim_movie   │ (joins all sources)
                    └───────┬───────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│fact_metrics   │   │bridge_cast    │   │bridge_genres  │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            ▼
                    ┌───────────────┐
                    │load_postgres  │
                    └───────────────┘
```

---

## EMR Cluster Tuning (1 Primary + 1 Core)

```bash
# Spark configuration for small cluster
--conf spark.executor.memory=4g
--conf spark.executor.cores=2
--conf spark.driver.memory=2g
--conf spark.sql.shuffle.partitions=20
--conf spark.sql.adaptive.enabled=true
--conf spark.dynamicAllocation.enabled=false
```

For large datasets (IMDb has ~10M titles), consider:
- Increase `shuffle.partitions` to 50-100
- Use `.repartition()` before writes
- Filter early (e.g., `titleType = 'movie'`)

---

## Next Steps

1. [ ] Create `processing/` folder structure
2. [ ] Implement `lib/` shared utilities first
3. [ ] Start with Bronze jobs (simplest)
4. [ ] Build entity spine (critical for joins)
5. [ ] Implement Silver cleaning jobs
6. [ ] Build Gold dimensions/facts
7. [ ] Test locally with sample data before EMR
8. [ ] Create deployment scripts

---

*Generated: 2026-01-31*
