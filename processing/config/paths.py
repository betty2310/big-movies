S3_BUCKET = "movies-datalake-2310"
S3_BASE = f"s3://{S3_BUCKET}"

RAW_PATH = f"{S3_BASE}/raw"
BRONZE_PATH = f"{S3_BASE}/bronze"
SILVER_PATH = f"{S3_BASE}/silver"
GOLD_PATH = f"{S3_BASE}/gold"

RAW_MOVIELENS = f"{RAW_PATH}/movielens"
RAW_IMDB = f"{RAW_PATH}/imdb"
RAW_TMDB = f"{RAW_PATH}/tmdb"
RAW_ROTTEN_TOMATOES = f"{RAW_PATH}/rotten_tomatoes"

BRONZE_MOVIELENS = f"{BRONZE_PATH}/movielens"
BRONZE_IMDB = f"{BRONZE_PATH}/imdb"
BRONZE_TMDB = f"{BRONZE_PATH}/tmdb"
BRONZE_ROTTEN_TOMATOES = f"{BRONZE_PATH}/rotten_tomatoes"
