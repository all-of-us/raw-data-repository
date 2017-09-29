#!/bin/bash -e

# Exports database tables to CSV files in GCS.

USAGE="tools/export_tables.sh --project <PROJECT> --account <ACCOUNT> --bucket <BUCKET> --directory <DIRECTORY> --database <DATABASE> --tables <TABLES> [--creds_account <ACCOUNT>]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --bucket) BUCKET=$2; shift 2;;
    --directory) DIRECTORY=$2; shift 2;;
    --database) DATABASE=$2; shift 2;;
    --tables) TABLES=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ] || [ -z "${PROJECT}" ] || [ -z "${BUCKET}" ] || [ -z "${DATABASE}" ] \
  || [ -z "${TABLES}" ] || [ -z "${DIRECTORY}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi

if [ -z "${CREDS_ACCOUNT}" ]
then
  CREDS_ACCOUNT="${ACCOUNT}"
fi

source tools/auth_setup.sh
SQL_SERVICE_ACCOUNT=`gcloud sql instances describe --project ${PROJECT} --account ${ACCOUNT} \
  rdrmaindb | grep serviceAccountEmailAddress | cut -d: -f2`
gsutil acl ch -u ${SQL_SERVICE_ACCOUNT}:W gs://${BUCKET}

REPO_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd )"
export PYTHONPATH=${PYTHONPATH}:${REPO_ROOT_DIR}/rdr_common

python tools/export_tables.py --project ${PROJECT} --creds_file ${CREDS_FILE} \
  --output_path gs://${BUCKET}/${DIRECTORY} --database ${DATABASE} --tables ${TABLES}

