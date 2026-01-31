#!/bin/bash
set -e

CLUSTER_ID="${1:?Usage: $0 <cluster-id>}"

S3_BUCKET="movies-datalake-2310"
S3_JOBS="s3://${S3_BUCKET}/processing/jobs/silver"

echo "=== Running Silver Layer Jobs on EMR Cluster: ${CLUSTER_ID} ==="

echo ""
echo "Step 1: Building Entity Spine (must complete first)"
STEP_ID=$(aws emr add-steps --cluster-id "${CLUSTER_ID}" --steps '[
  {"Name":"Silver-EntitySpine","ActionOnFailure":"CANCEL_AND_WAIT","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/build_entity_spine.py"]}
]' --query 'StepIds[0]' --output text)

echo "Entity Spine step submitted: ${STEP_ID}"
echo "Waiting for Entity Spine to complete..."

aws emr wait step-complete --cluster-id "${CLUSTER_ID}" --step-id "${STEP_ID}"
echo "✓ Entity Spine complete"

echo ""
echo "Step 2: Running cleaning jobs in parallel"
aws emr add-steps --cluster-id "${CLUSTER_ID}" --steps '[
  {"Name":"Silver-MovieLens","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/clean_movielens.py"]},
  {"Name":"Silver-IMDb","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/clean_imdb.py"]},
  {"Name":"Silver-TMDB","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/clean_tmdb.py"]},
  {"Name":"Silver-RottenTomatoes","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar",
   "Args":["spark-submit","--deploy-mode","cluster","'${S3_JOBS}'/clean_rotten_tomatoes.py"]}
]'

echo ""
echo "=== Silver jobs submitted ==="
echo "Monitor at: https://console.aws.amazon.com/emr/home#/clusterDetails/${CLUSTER_ID}"
