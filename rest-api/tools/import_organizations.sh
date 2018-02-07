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
if [ -z "${ACCOUNT}" ]  && [ "${PROJECT}" ];
then
  echo "Usage: $USAGE"
  exit 1
fi

TMP_GEOCODE_DIR=$(mktemp -d)
TMP_GEOCODE_INFO_FILE=${TMP_GEOCODE_DIR}/geocode_key.json

function cleanup {
:
}

function get_geocode_key {
    echo "Getting geocode api key ..."
    (tools/install_config.sh --key geocode_key --account "${ACCOUNT}" \
	    --project "pmi-drc-api-test"  --config_output "$TMP_GEOCODE_INFO_FILE")
    export API_KEY=$(cat $TMP_GEOCODE_INFO_FILE | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["'api_key'"]')
}

CREDS_ACCOUNT="${ACCOUNT}"
if [ -z "${ACCOUNT}" ]
then
echo "Not Geocoding addresses without --account"
else
GEOCODE_FLAG=--geocode_flag
get_geocode_key
fi

if [ "${PROJECT}" ]
then
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
  --organization_file data/organizations.csv --site_file data/sites.csv $DRY_RUN $GEOCODE_FLAG

function finish {
  cleanup
  rm -rf ${TMP_GEOCODE_DIR}
  rm -f ${TMP_GEOCODE_INFO_FILE}
}
trap finish EXIT

