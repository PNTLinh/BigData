#!/bin/bash
set -e

DATASET_NAME=$1
VERSION=$2
URL_PREFIX="https://raw.githubusercontent.com/your-username/fraud-dataset-repo/main/data"

for PART in {1..5}; do
  FPART=`printf "%02d" ${PART}`

  URL="${URL_PREFIX}/${DATASET_NAME}/transactions_part_${FPART}.csv.gz"

  LOCAL_PREFIX="data/raw/transactions/${DATASET_NAME}/${VERSION}"
  LOCAL_FILE="fraud_data_part_${FPART}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "Downloading financial data from ${URL} to ${LOCAL_PATH}..."

  mkdir -p ${LOCAL_PREFIX}

  wget ${URL} -O ${LOCAL_PATH} || echo "Warning: Could not download part ${FPART}"

done

echo "Download complete. Data is ready for Prefect ingestion to GCS."