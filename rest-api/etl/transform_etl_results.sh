#!/bin/bash -e

# Transforms ETL results in GCS so that "N (null values in current Cloud SQL export) are replaced
# with an empty string. Output files are written to a "transformed" subdirectory.
#
# We will stop using this script once the Cloud SQL export API provides the option to specify a
# different null value.

USAGE="etl/transform_etl_results.sh --project <PROJECT> --account <ACCOUNT> --directory <DIRECTORY>"

while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --directory) DIRECTORY=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ] || [ -z "${PROJECT}" ] || [ -z "${DIRECTORY}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi
CREDS_ACCOUNT=${ACCOUNT}
CSV_DIR=/tmp/rdr-export-csv

source tools/auth_setup.sh

function finish {
  rm -rf ${CSV_DIR}
  cleanup
}
trap finish EXIT

echo "Activating service account..."
gcloud iam service-accounts keys create $CREDS_FILE --iam-account=$SERVICE_ACCOUNT --account=$ACCOUNT
gcloud auth activate-service-account $SERVICE_ACCOUNT --key-file=$CREDS_FILE

echo "Copying CSV files from GCS..."
mkdir -p ${CSV_DIR}
mkdir -p ${CSV_DIR}/transformed

CLOUD_DIR=gs://${PROJECT}-cdm/${DIRECTORY}
gsutil cp ${CLOUD_DIR}/*.csv ${CSV_DIR}

echo "Transforming CSV files..."
for file in ${CSV_DIR}/*.csv
do
    filename=$(basename "$file")
    # Replace "N with empty string, but only when followed by a comma and then a comma, quote,
    # or number, and not ([0-9],)*[0-9]- (which appear in concept_synonym)
    cat $file | perl -pe 's/\"N,(?=[,\"0-9])(?!([0-9],)*[0-9]-)/,/g' | sed 's/\"N$//g' > ${CSV_DIR}/transformed/$filename
done

echo "Uploading files back to GCS..."
gsutil cp -r ${CSV_DIR}/transformed ${CLOUD_DIR}

echo "Done."