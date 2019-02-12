#!/bin/bash -ae

# Prepares databases in Cloud SQL for running the RDR -> OMOP ETL.
# Imports "cdm" and "voc" databases located in GCS, which were produced by running
# setup_local_database_for_etl.
# If the databases already exist, drop them prior to running this.
# Note: this takes a while. Go get some coffee while it's running!

USAGE="tools/setup_database_for_etl.sh --project <PROJECT> --account <ACCOUNT> --src-folder <FOLDER> "
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --src-folder) SRCFOLDER=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ] || [ -z "${PROJECT}" ] || [ -z "${SRCFOLDER}" ]; then
  echo "Usage: $USAGE"
  exit 1
fi

CREDS_ACCOUNT=${ACCOUNT}

SRCBUCKET="all-of-us-rdr-vocabulary"

echo ""
echo "  source folder:  ${SRCBUCKET}/${SRCFOLDER}"

echo
while true; do
    read -p "Is this source correct? (y/n) " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer y or n.";;
    esac
done
echo "running..."

source tools/auth_setup.sh
get_db_password "${ROOT_DB_USER}"

run_cloud_sql_proxy

echo "Creating voc and cdm databases..."
mysql --verbose -h 127.0.0.1 -u "${ROOT_DB_USER}" -p${PASSWORD} --port ${PORT} < etl/create_dbs.sql

SQL_SERVICE_ACCOUNT=`gcloud sql instances describe --project ${PROJECT} --account ${ACCOUNT} \
rdrmaindb | grep serviceAccountEmailAddress | cut -d: -f2`

echo "Granting GCS access to ${SQL_SERVICE_ACCOUNT}..."
gsutil acl ch -u ${SQL_SERVICE_ACCOUNT}:W gs://${SRCBUCKET}
gsutil acl ch -u ${SQL_SERVICE_ACCOUNT}:R gs://${SRCBUCKET}/${SRCFOLDER}/*.sql

echo "Importing CDM database..."
# WARNING: This command is deprecated and will be removed in version 205.0.0. Use `gcloud sql import sql` instead.
gcloud sql instances import --quiet --project ${PROJECT} --account ${ACCOUNT} rdrmaindb gs://${SRCBUCKET}/${SRCFOLDER}/cdm.sql

echo "Importing VOC database..."
gcloud sql instances import --quiet --project ${PROJECT} --account ${ACCOUNT} rdrmaindb gs://${SRCBUCKET}/${SRCFOLDER}/voc.sql

echo "Done."
