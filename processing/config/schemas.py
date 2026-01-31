from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    ArrayType,
)

MOVIELENS_RATINGS_SCHEMA = StructType([
    StructField("userId", IntegerType(), False),
    StructField("movieId", IntegerType(), False),
    StructField("rating", FloatType(), False),
    StructField("timestamp", LongType(), False),
])

MOVIELENS_MOVIES_SCHEMA = StructType([
    StructField("movieId", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("genres", StringType(), True),
])

MOVIELENS_TAGS_SCHEMA = StructType([
    StructField("userId", IntegerType(), False),
    StructField("movieId", IntegerType(), False),
    StructField("tag", StringType(), True),
    StructField("timestamp", LongType(), False),
])

MOVIELENS_LINKS_SCHEMA = StructType([
    StructField("movieId", IntegerType(), False),
    StructField("imdbId", StringType(), True),
    StructField("tmdbId", IntegerType(), True),
])

IMDB_TITLE_BASICS_SCHEMA = StructType([
    StructField("tconst", StringType(), False),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", StringType(), True),
    StructField("startYear", StringType(), True),
    StructField("endYear", StringType(), True),
    StructField("runtimeMinutes", StringType(), True),
    StructField("genres", StringType(), True),
])

IMDB_TITLE_RATINGS_SCHEMA = StructType([
    StructField("tconst", StringType(), False),
    StructField("averageRating", StringType(), True),
    StructField("numVotes", StringType(), True),
])

IMDB_NAME_BASICS_SCHEMA = StructType([
    StructField("nconst", StringType(), False),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", StringType(), True),
    StructField("deathYear", StringType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True),
])

IMDB_TITLE_PRINCIPALS_SCHEMA = StructType([
    StructField("tconst", StringType(), False),
    StructField("ordering", StringType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True),
])

IMDB_TITLE_CREW_SCHEMA = StructType([
    StructField("tconst", StringType(), False),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True),
])

IMDB_TITLE_AKAS_SCHEMA = StructType([
    StructField("titleId", StringType(), False),
    StructField("ordering", StringType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", StringType(), True),
    StructField("attributes", StringType(), True),
    StructField("isOriginalTitle", StringType(), True),
])

IMDB_TITLE_EPISODE_SCHEMA = StructType([
    StructField("tconst", StringType(), False),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", StringType(), True),
    StructField("episodeNumber", StringType(), True),
])

TMDB_MOVIE_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("poster_path", StringType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("genre_ids", ArrayType(IntegerType()), True),
    StructField("original_language", StringType(), True),
    StructField("adult", BooleanType(), True),
    StructField("video", BooleanType(), True),
])

ROTTEN_TOMATOES_SCHEMA = StructType([
    StructField("rt_id", StringType(), False),
    StructField("slug", StringType(), True),
    StructField("title", StringType(), True),
    StructField("tomatometer_score", IntegerType(), True),
    StructField("audience_score", IntegerType(), True),
    StructField("mpaa_rating", StringType(), True),
    StructField("box_office", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("director", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("scraped_at", StringType(), True),
])
