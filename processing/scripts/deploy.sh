#!/bin/bash
set -e

S3_BUCKET="movies-datalake-2310"
S3_PROCESSING="s3://${S3_BUCKET}/processing"

echo "=== Deploying Spark jobs to S3 ==="

cd "$(dirname "$0")/.."

echo "Uploading jobs to S3..."
aws s3 sync jobs/ "${S3_PROCESSING}/jobs/" --exclude "*.pyc" --exclude "__pycache__/*"

echo ""
echo "=== Deployment complete ==="
echo "Jobs available at: ${S3_PROCESSING}/jobs/"
