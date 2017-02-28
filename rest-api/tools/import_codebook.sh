#!/bin/bash

# Retrieves a codebook from a specified URL and imports it into the database.
# Can be used for either your local database or Cloud SQL.

USAGE="tools/import_codebook.sh [--url <URL>] [--account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>]]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --url) URL=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ "${PROJECT}" ]
then
  if [ -z "${ACCOUNT}" ]
  then
    echo "Usage: $USAGE"
    exit 1
  fi
  if [ -z "${CREDS_ACCOUNT}" ]
  then
    CREDS_ACCOUNT="${ACCOUNT}"
  fi
  source tools/auth_setup.sh
  run_cloud_sql_proxy
  set_db_connection_string
fi
if [ -z "${URL}" ]
then
  # TODO -- update with the URL for the latest released version of the codebook
  URL="https://raw.githubusercontent.com/jmandel/codebook-to-fhir/v0.0.1/CodeSystem/ppi.json"
fi

CODEBOOK_FILE=/tmp/pmi-codebook.json

echo "Fetching codebook from ${URL}..."
wget -O $CODEBOOK_FILE $URL

(source tools/set_path.sh; python tools/import_codebook.py --file $CODEBOOK_FILE)
