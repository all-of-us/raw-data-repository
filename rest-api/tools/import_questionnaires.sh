#!/bin/bash -e

# Retrieves questionnaires from a URL and imports them into the database.
# Can be used for either your local database or Cloud SQL.

USAGE="tools/import_questionnaires.sh [--url <URL>] [--account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>]]"
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
else
  if [ -z "${DB_CONNECTION_STRING}" ]
  then
    source tools/setup_local_vars.sh
    set_local_db_connection_string
  fi
fi

QUESTIONNAIRE_FILES=(base_consent.json basics.json ehr_consent.json lifestyle.json overall_health.json)
QUESTIONNAIRE_TMP_DIR=/tmp/rdr-questionnaires/
echo "Fetching questionnaires from github..."
mkdir -p ${QUESTIONNAIRE_TMP_DIR}
# Fetch the questionnaires from github and write them to the questionnaire dir.
for file in "${QUESTIONNAIRE_FILES[@]}";
do
  curl "https://raw.githubusercontent.com/all-of-us-terminology/api-payloads/master/questionnaire_payloads/${file}" > ${QUESTIONNAIRE_TMP_DIR}${file}
done

QUESTIONNAIRE_FILES_STR=$(IFS=, ; echo "${QUESTIONNAIRE_FILES[*]}")
QUESTIONNAIRE_DIR=test/test-data/

echo "Importing questionnaires..."
(source tools/set_path.sh; python tools/import_questionnaires.py --dir $QUESTIONNAIRE_TMP_DIR \
 --files $QUESTIONNAIRE_FILES_STR)
