#!/bin/bash -e

# Generates a spec for questionnaire response answers used when generating fake PPI data

USAGE="tools/generate_questionnaire_response_answer_spec.sh --output_file <OUTPUT_FILE> [--account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>]]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --output_file) OUTPUT_FILE=$2; shift 2;;
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

if [ -z "${OUTPUT_FILE}" ]
then 
  echo "Usage: $USAGE"
fi

source tools/set_path.sh
python tools/generate_questionnaire_response_answer_spec.py --output_file $OUTPUT_FILE
