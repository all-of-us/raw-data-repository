#!/bin/bash -e

# Starts Cloud SQL proxy, connected to the Cloud SQL instance for a particular environment,
# and runs mysql to connect to it.

USAGE="tools/connect_to_database.sh --account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>] \
  [--user <USER> --password <PASSWORD>] [--command <COMMAND> [--output <OUTPUT_FILE>]]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --user) CONNECT_USER=$2; shift 2;;
    --command) COMMAND=$2; shift 2;;
    --output_csv) OUTPUT=$2; shift 2;;
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

if [ -z $CONNECT_USER ]
then
  CONNECT_USER="${DB_USER}"
fi

get_db_password $CONNECT_USER
CONNECT_PASSWORD="${PASSWORD}"
run_cloud_sql_proxy

SUFFIX=
if [ "${COMMAND}" ]
then
  SUFFIX="-e \"${COMMAND}\""
fi

SUFFIX2=
if [ "${OUTPUT}" ]
then
  # Convert the output of MySQL tables to CSV format.
  SUFFIX2=" | sed 's/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g' > ${OUTPUT}"
fi

MYSQL_COMMAND="mysql -u \"${CONNECT_USER}\" -p\"${CONNECT_PASSWORD}\" -h 127.0.0.1 --port ${PORT} -D \"${DB_NAME}\" ${SUFFIX} ${SUFFIX2}"
eval ${MYSQL_COMMAND}


