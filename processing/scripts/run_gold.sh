#!/bin/bash
set -e

CLUSTER_ID="${1:?Usage: $0 <cluster-id>}"

S3_BUCKET="movies-datalake-2310"
S3_JOBS="s3://${S3_BUCKET}/processing/jobs/gold"

echo "=== Running Gold Layer Jobs on EMR Cluster: ${CLUSTER_ID} ==="

echo ""
echo "Step 1: Building independent dimensions (parallel)"
aws emr add-steps --cluster-id "${CLUSTER_ID}" --steps '[
  {"Name":"Gold-DimTime","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/build_dim_time.py"]},
  {"Name":"Gold-DimGenre","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/build_dim_genre.py"]},
  {"Name":"Gold-DimPerson","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/build_dim_person.py"]}
]'

echo ""
echo "Waiting for dimension builds to complete..."
sleep 10

echo ""
echo "Step 2: Building dim_movie (depends on all sources)"
STEP_ID=$(aws emr add-steps --cluster-id "${CLUSTER_ID}" --steps '[
  {"Name":"Gold-DimMovie","ActionOnFailure":"CANCEL_AND_WAIT","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/build_dim_movie.py"]}
]' --query 'StepIds[0]' --output text)

echo "DimMovie step submitted: ${STEP_ID}"
echo "Waiting for dim_movie to complete..."
aws emr wait step-complete --cluster-id "${CLUSTER_ID}" --step-id "${STEP_ID}"
echo "✓ dim_movie complete"

echo ""
echo "Step 3: Building fact table and bridge tables (parallel)"
aws emr add-steps --cluster-id "${CLUSTER_ID}" --steps '[
  {"Name":"Gold-FactMetrics","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/build_fact_metrics.py"]},
  {"Name":"Gold-BridgeCast","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/build_bridge_cast.py"]},
  {"Name":"Gold-BridgeGenres","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/build_bridge_genres.py"]}
]'

echo ""
echo "=== Gold layer jobs submitted ==="
echo "Monitor at: https://console.aws.amazon.com/emr/home#/clusterDetails/${CLUSTER_ID}"
