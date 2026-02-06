-- Big Movies Data Warehouse Schema
-- Generated from gold layer parquet definitions
-- Run this in Supabase SQL Editor
-- All columns nullable for fault tolerance (except PKs)

-- ============================================================================
-- Drop existing tables (in reverse dependency order)
-- ============================================================================
DROP TABLE IF EXISTS bridge_movie_cast CASCADE;
DROP TABLE IF EXISTS bridge_movie_genres CASCADE;
DROP TABLE IF EXISTS fact_movie_metrics CASCADE;
DROP TABLE IF EXISTS dim_movie CASCADE;
DROP TABLE IF EXISTS dim_person CASCADE;
DROP TABLE IF EXISTS dim_genre CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;

-- ============================================================================
-- Dimension Tables
-- ============================================================================

-- Time Dimension (1950-2030)
CREATE TABLE dim_time (
    time_id         INTEGER PRIMARY KEY,
    date            DATE,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    day             INTEGER,
    day_of_week     INTEGER,
    day_of_year     INTEGER,
    week_of_year    INTEGER
);

CREATE INDEX idx_dim_time_year ON dim_time(year);
CREATE INDEX idx_dim_time_date ON dim_time(date);

-- Genre Dimension
CREATE TABLE dim_genre (
    genre_id        INTEGER PRIMARY KEY,
    genre_name      TEXT
);

-- Person Dimension (actors, directors, crew from IMDb)
CREATE TABLE dim_person (
    person_id           TEXT PRIMARY KEY,
    name                TEXT,
    birth_year          INTEGER,
    death_year          INTEGER,
    primary_profession  TEXT,
    all_professions     TEXT,
    known_for_titles    TEXT
);

CREATE INDEX idx_dim_person_name ON dim_person(name);
CREATE INDEX idx_dim_person_birth_year ON dim_person(birth_year);

-- Movie Dimension (unified from all sources)
-- movie_id is BIGINT (from Spark long type)
CREATE TABLE dim_movie (
    movie_id        BIGINT PRIMARY KEY,
    imdb_id         TEXT,
    tmdb_id         INTEGER,
    ml_id           INTEGER,
    rt_slug         TEXT,
    title           TEXT,
    original_title  TEXT,
    year            INTEGER,
    runtime         INTEGER,
    mpaa_rating     TEXT,
    plot_summary    TEXT,
    poster_url      TEXT,
    backdrop_url    TEXT,
    is_adult        INTEGER,
    popularity      DOUBLE PRECISION
);

CREATE INDEX idx_dim_movie_imdb_id ON dim_movie(imdb_id);
CREATE INDEX idx_dim_movie_tmdb_id ON dim_movie(tmdb_id);
CREATE INDEX idx_dim_movie_ml_id ON dim_movie(ml_id);
CREATE INDEX idx_dim_movie_year ON dim_movie(year);
CREATE INDEX idx_dim_movie_title ON dim_movie(title);

-- ============================================================================
-- Fact Table
-- ============================================================================

-- Movie Metrics Fact (all ratings and metrics aggregated)
CREATE TABLE fact_movie_metrics (
    metric_id           BIGINT PRIMARY KEY,
    movie_id            BIGINT REFERENCES dim_movie(movie_id),
    time_id             INTEGER REFERENCES dim_time(time_id),
    ml_avg_rating       DOUBLE PRECISION,
    ml_rating_count     INTEGER,
    imdb_rating         DOUBLE PRECISION,
    imdb_votes          INTEGER,
    tmdb_rating         DOUBLE PRECISION,
    tmdb_votes          INTEGER,
    tmdb_popularity     DOUBLE PRECISION,
    tomatometer_score   INTEGER,
    audience_score      INTEGER,
    divisive_score      INTEGER,
    budget              BIGINT,
    revenue             BIGINT
);

CREATE INDEX idx_fact_movie_metrics_movie_id ON fact_movie_metrics(movie_id);
CREATE INDEX idx_fact_movie_metrics_time_id ON fact_movie_metrics(time_id);

-- ============================================================================
-- Bridge Tables (Many-to-Many)
-- ============================================================================

-- Movie to Cast/Crew relationship
CREATE TABLE bridge_movie_cast (
    movie_id        BIGINT NOT NULL REFERENCES dim_movie(movie_id),
    person_id       TEXT NOT NULL REFERENCES dim_person(person_id),
    category        TEXT,
    ordering        INTEGER,
    job             TEXT,
    characters      TEXT,
    PRIMARY KEY (movie_id, person_id, ordering)
);

CREATE INDEX idx_bridge_movie_cast_movie_id ON bridge_movie_cast(movie_id);
CREATE INDEX idx_bridge_movie_cast_person_id ON bridge_movie_cast(person_id);
CREATE INDEX idx_bridge_movie_cast_category ON bridge_movie_cast(category);

-- Movie to Genre relationship
CREATE TABLE bridge_movie_genres (
    movie_id        BIGINT NOT NULL REFERENCES dim_movie(movie_id),
    genre_id        INTEGER NOT NULL REFERENCES dim_genre(genre_id),
    PRIMARY KEY (movie_id, genre_id)
);

CREATE INDEX idx_bridge_movie_genres_movie_id ON bridge_movie_genres(movie_id);
CREATE INDEX idx_bridge_movie_genres_genre_id ON bridge_movie_genres(genre_id);

-- ============================================================================
-- Useful Views
-- ============================================================================

CREATE OR REPLACE VIEW v_movie_full AS
SELECT
    m.movie_id,
    m.title,
    m.original_title,
    m.year,
    m.runtime,
    m.mpaa_rating,
    m.plot_summary,
    m.poster_url,
    f.ml_avg_rating,
    f.ml_rating_count,
    f.imdb_rating,
    f.imdb_votes,
    f.tmdb_rating,
    f.tmdb_votes,
    f.tomatometer_score,
    f.audience_score,
    f.divisive_score,
    f.budget,
    f.revenue
FROM dim_movie m
LEFT JOIN fact_movie_metrics f ON m.movie_id = f.movie_id;

CREATE OR REPLACE VIEW v_movie_genres AS
SELECT
    m.movie_id,
    m.title,
    m.year,
    STRING_AGG(g.genre_name, ', ' ORDER BY g.genre_name) AS genres
FROM dim_movie m
LEFT JOIN bridge_movie_genres bg ON m.movie_id = bg.movie_id
LEFT JOIN dim_genre g ON bg.genre_id = g.genre_id
GROUP BY m.movie_id, m.title, m.year;

CREATE OR REPLACE VIEW v_movie_cast AS
SELECT
    m.movie_id,
    m.title,
    bc.category,
    bc.ordering,
    p.person_id,
    p.name AS person_name,
    bc.characters
FROM dim_movie m
JOIN bridge_movie_cast bc ON m.movie_id = bc.movie_id
JOIN dim_person p ON bc.person_id = p.person_id;

-- ============================================================================
-- RLS Policies
-- ============================================================================

ALTER TABLE dim_time ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_genre ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_person ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_movie ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_movie_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE bridge_movie_cast ENABLE ROW LEVEL SECURITY;
ALTER TABLE bridge_movie_genres ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow read for authenticated" ON dim_time FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow read for authenticated" ON dim_genre FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow read for authenticated" ON dim_person FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow read for authenticated" ON dim_movie FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow read for authenticated" ON fact_movie_metrics FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow read for authenticated" ON bridge_movie_cast FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow read for authenticated" ON bridge_movie_genres FOR SELECT TO authenticated USING (true);

CREATE POLICY "Allow read for anon" ON dim_time FOR SELECT TO anon USING (true);
CREATE POLICY "Allow read for anon" ON dim_genre FOR SELECT TO anon USING (true);
CREATE POLICY "Allow read for anon" ON dim_person FOR SELECT TO anon USING (true);
CREATE POLICY "Allow read for anon" ON dim_movie FOR SELECT TO anon USING (true);
CREATE POLICY "Allow read for anon" ON fact_movie_metrics FOR SELECT TO anon USING (true);
CREATE POLICY "Allow read for anon" ON bridge_movie_cast FOR SELECT TO anon USING (true);
CREATE POLICY "Allow read for anon" ON bridge_movie_genres FOR SELECT TO anon USING (true);
