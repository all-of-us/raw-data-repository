#!/bin/bash -e

# Upserts HPO sites from a CSV input file.
# The CSV is from the site information spreadsheet, "Sites" tab:
# https://docs.google.com/spreadsheets/d/1AbumEBdalefpxNJaWOu4bMHdzUiVpezurGH5-w9Bv4k

USAGE="tools/import_sites.sh --file <FILE> [--account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>]]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --file) FILE=$2; shift 2;;
    --dry_run) DRY_RUN=--dry_run; shift 1;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${FILE}" ]
then
  echo "--file is required. Usage: $USAGE"
  exit 1
fi

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

source tools/set_path.sh
python tools/import_sites.py --file $FILE $DRY_RUN
