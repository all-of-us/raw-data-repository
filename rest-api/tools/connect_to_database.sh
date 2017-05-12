#!/bin/bash

# Starts Cloud SQL proxy, connected to the Cloud SQL instance for a particular environment,
# and runs mysql to connect to it.
#

USAGE="tools/connect_to_database.sh --account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>] [--user <USER> --password <PASSWORD>]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --user) CONNECT_USER=$2; shift 2;;
    --password) CONNECT_PASSWORD=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ] || [ -z "${PROJECT} " ]
then
  echo "Usage: $USAGE"
  exit 1
fi
if [ -z "${CREDS_ACCOUNT}" ]
then
  CREDS_ACCOUNT="${ACCOUNT}"
fi

source tools/setup_vars.sh
source tools/auth_setup.sh
get_db_password
run_cloud_sql_proxy

if [ -z $CONNECT_USER ]
then
  CONNECT_USER="${DB_USER}"
  CONNECT_PASSWORD="${PASSWORD}"
fi

mysql -u "root" -p"$CONNECT_PASSWORD" --host 127.0.0.1 --port ${PORT} "$DB_NAME"
