#!/bin/bash -e

# Upserts awardees, organizations, and/or sites from CSV input files in the data dir

USAGE="tools/import_organizations.sh [--account <USER>@pmi-ops.org --project <PROJECT>] [--dry_run]"

while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --dry_run) DRY_RUN=--dry_run; shift 1;;
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
  CREDS_ACCOUNT="${ACCOUNT}"
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
python tools/import_organizations.py --awardee_file data/awardees.csv \
  --organization_file data/organizations.csv --site_file data/sites.csv $DRY_RUN
