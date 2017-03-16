#!/bin/bash

# Sets up a Cloud SQL instance, sets the root password, creates an empty database,
# and sets the database connection info in a database config in Datastore.
#
# Note that this script does *not* set up failover for the database. You can do that manually
# in the environments you want.
#
# creds_account can be provided in cases where the user associated with the service account
# needed to update configuration differs from the account that can update AppEngine for the instance
#
# create_instance can be provided to create the database instance the first time

# Example usage:
# tools/setup_database.sh --account dan.rodney@pmi-ops.org --project all-of-us-rdr-staging

CREATE_INSTANCE=
USAGE="tools/setup_database.sh --account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>] [--create_instance]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --create_instance) CREATE_INSTANCE=Y; shift 1;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ]
then
  echo "Usage: $USAGE"
xit 1
fi

if [ -z "${PROJECT}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi

if [ -z "${CREDS_ACCOUNT}" ]
then
  CREDS_ACCOUNT="${ACCOUNT}"
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

INSTANCE_NAME=rdrmaindb
FAILOVER_INSTANCE_NAME=rdrbackupdb
DB_USER=root
DB_NAME=rdr
# The default configuration; uses a non-shared CPU, with 8 cores and 30 GB of memory
# (Consider making this something different in production.)
MACHINE_TYPE=db-n1-standard-8

source tools/auth_setup.sh

if [ "${CREATE_INSTANCE}" = "Y" ]
then
  gcloud beta sql instances create $INSTANCE_NAME --tier=$MACHINE_TYPE --activation-policy=ALWAYS \
      --backup-start-time 00:00 --failover-replica-name $FAILOVER_INSTANCE_NAME --enable-bin-log \
      --database-version MYSQL_5_7 --project $PROJECT
  sleep 3
fi
gcloud sql instances set-root-password $INSTANCE_NAME --password $PASSWORD

INSTANCE_CONNECTION_NAME=$(gcloud sql instances describe $INSTANCE_NAME | grep connectionName | cut -f2 -d' ')
CONNECTION_STRING="mysql+mysqldb://${DB_USER}:${PASSWORD}@/$DB_NAME?unix_socket=/cloudsql/$INSTANCE_CONNECTION_NAME&charset=utf8"

CREATE_DB_FILE=/tmp/create_db.sql

function finish {
  cleanup
  rm -f ${CREATE_DB_FILE}
}
trap finish EXIT

echo '{"db_connection_string": "'$CONNECTION_STRING'", ' \
     ' "db_password": "'$PASSWORD'", ' \
     ' "db_connection_name": "'$INSTANCE_CONNECTION_NAME'", '\
     ' "db_user": "'$DB_USER'", '\
     ' "db_name": "'$DB_NAME'" }' > $DB_INFO_FILE

echo 'CREATE DATABASE IF NOT EXISTS '$DB_NAME > $CREATE_DB_FILE

run_cloud_sql_proxy

echo "Creating empty database..."
mysql -u "$DB_USER" -p"$PASSWORD" --host 127.0.0.1 --port ${PORT} < ${CREATE_DB_FILE}

echo "Setting database configuration"
tools/install_config.sh --key db_config --config ${DB_INFO_FILE} --instance $INSTANCE --update --creds_file ${CREDS_FILE}

