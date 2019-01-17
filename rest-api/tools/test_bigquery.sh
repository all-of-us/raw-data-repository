#!/usr/bin/env bash

#!/bin/bash -ae

# Test a sql query using google big query.

USAGE="tools/test_bigquery.sh --account <ACCOUNT> --project <PROJECT> --sql <SQL STMT>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --sql) SQL=$2; shift 2;;
    * ) break ;;
  esac
done

if [[ -z "${ACCOUNT}" ]] || [[ -z "${PROJECT}" ]] || [[ -z "${SQL}" ]]
then
  echo "Usage: ${USAGE}"
  exit 1
fi

CREDS_ACCOUNT="${ACCOUNT}"
SERVICE_ACCOUNT="${PROJECT}@appspot.gserviceaccount.com"
source tools/set_path.sh
source tools/auth_setup.sh
gcloud auth activate-service-account ${SERVICE_ACCOUNT} --key-file ${CREDS_FILE}

bq query --nouse_legacy_sql $SQL


