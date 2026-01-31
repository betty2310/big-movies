#!/bin/bash
set -e

CLUSTER_ID="${1:?Usage: $0 <cluster-id> [input-date]}"
INPUT_DATE="${2:-}"

S3_BUCKET="movies-datalake-2310"
S3_JOBS="s3://${S3_BUCKET}/processing/jobs/bronze"

echo "=== Running Bronze Layer Jobs on EMR Cluster: ${CLUSTER_ID} ==="

if [ -n "$INPUT_DATE" ]; then
    MOVIELENS_ARGS='["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/ingest_movielens.py","--input-date","'${INPUT_DATE}'"]'
    IMDB_ARGS='["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/ingest_imdb.py","--input-date","'${INPUT_DATE}'"]'
    TMDB_ARGS='["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/ingest_tmdb.py","--input-date","'${INPUT_DATE}'"]'
    RT_ARGS='["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/ingest_rotten_tomatoes.py","--input-date","'${INPUT_DATE}'"]'
else
    MOVIELENS_ARGS='["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/ingest_movielens.py"]'
    IMDB_ARGS='["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/ingest_imdb.py"]'
    TMDB_ARGS='["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/ingest_tmdb.py"]'
    RT_ARGS='["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/ingest_rotten_tomatoes.py"]'
fi

aws emr add-steps --cluster-id "${CLUSTER_ID}" --steps '[
  {"Name":"Bronze-MovieLens","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Args":'"${MOVIELENS_ARGS}"'},
  {"Name":"Bronze-IMDb","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Args":'"${IMDB_ARGS}"'},
  {"Name":"Bronze-TMDB","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Args":'"${TMDB_ARGS}"'},
  {"Name":"Bronze-RottenTomatoes","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Args":'"${RT_ARGS}"'}
]'

echo ""
echo "=== Bronze jobs submitted ==="
echo "Monitor at: https://console.aws.amazon.com/emr/home#/clusterDetails/${CLUSTER_ID}"
