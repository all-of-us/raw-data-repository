#!/bin/bash

# Sets up a Cloud SQL instance, sets the root password, and sets the password as an environment
# variable in CircleCI (which will get appended to app.yaml at deployment time, and used in
# Alembic migrations).
# Note that this script does *not* set up failover for the database. You can do that manually
# in the environments you want.

# Example usage:
# tools/setup_database.sh --account=dan.rodney@pmi-ops.org --project=all-of-us-rdr-staging \
#    --instance=https://all-of-us-rdr-staging.appspot.com/

CREATE_DB=
CREDS_FILE_ARGS=
USAGE="tools/setup_database.sh --account=<ACCOUNT> --project=<PROJECT> [--create_db] [--creds_file=<CREDS FILE>]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;    
    --create_db) CREATE_DB=Y; shift 1;;
    --creds_file) CREDS_FILE_ARGS="--creds_file=$2"; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ]
  then
    echo "Usage: $USAGE"
	exit 1
fi

if [ -z "${PROJECT}" ]
  then
    echo "Usage: $USAGE"
    exit 1
fi

read -s -p "root password for database: " PASSWORD
echo
if [ -z "${PASSWORD}" ]
  then
    echo "Password required; exiting."
    exit 1
fi

read -s -p "Repeat password: " REPEAT_PASSWORD
echo
if [ "${REPEAT_PASSWORD}" != "${PASSWORD}" ]
  then
    echo "Password mismatch; exiting."
    exit 1
fi

INSTANCE=https://${PROJECT}.appspot.com
INSTANCE_NAME=rdrmain
DB_USER=root
DB_NAME=rdr
# The default configuration; uses a non-shared CPU, with 8 cores and 30 GB of memory
# (Consider making this something different in production.)
MACHINE_TYPE=db-n1-standard-8

set -e
gcloud auth login $ACCOUNT
gcloud config set project $PROJECT
if [ "${CREATE_DB}" = "Y" ]
  then
    gcloud beta sql instances create $INSTANCE_NAME --tier=$MACHINE_TYPE  --activation-policy=ALWAYS
fi
gcloud sql instances set-root-password $INSTANCE_NAME --password $PASSWORD

INSTANCE_CONNECTION_NAME=$(gcloud sql instances describe $INSTANCE_NAME | grep connectionName | cut -f2 -d' ')
CONNECTION_STRING=mysql+mysqldb://${DB_USER}:${PASSWORD}@/$DB_NAME?unix_socket=/cloudsql/$INSTANCE_CONNECTION_NAME

function finish {
  rm -f /tmp/db_info.json
}
trap finish EXIT

echo '{"db_connection_string": "'$CONNECTION_STRING'", ' \
     ' "db_password": "'$PASSWORD'", ' \
     ' "db_connection_name": "'$INSTANCE_CONNECTION_NAME'", '\
     ' "db_user": "'$DB_USER'", '\
     ' "db_name": "'$DB_NAME'" }' > /tmp/db_info.json
echo "Setting database configuration"
set -x
tools/install_config.sh --key db_config --config /tmp/db_info.json --instance $INSTANCE --update $CREDS_FILE_ARGS



