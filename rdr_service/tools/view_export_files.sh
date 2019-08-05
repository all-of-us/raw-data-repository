#!/bin/bash -e

# Lists CSV files exported to the deidentified GCS bucket from RDR.

USAGE="tools/view_export_files.sh --directory <DIRECTORY> --account <ACCOUNT> --project <PROJECT>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --directory) DIRECTORY=$2; shift 2;;
    * ) break ;;
  esac
done

if [ -z "${DIRECTORY}" ] || [ -z "${PROJECT}" ] || [ -z "${ACCOUNT}" ]
then
  echo "Usage: ${USAGE}"
  exit 1
fi

CREDS_ACCOUNT="${ACCOUNT}"
SERVICE_ACCOUNT="exporter@${PROJECT}.iam.gserviceaccount.com"
source tools/auth_setup.sh
gcloud auth activate-service-account --key-file ${CREDS_FILE}
gsutil ls -l gs://${PROJECT}-deidentified-export/${DIRECTORY}
