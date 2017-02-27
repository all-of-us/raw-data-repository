#!/bin/bash

# Sets up a MySQL database named "rdr" locally (dropping the database if it already exists),
# and sets the database config information in the 
# local Datastore instance. You must have MySQL installed and running and your local
# dev_appserver instance running before using this.

# If you have an environment variable named "MYSQL_ROOT_PASSWORD" it will be used as the password
# to connect to the database; by default, the password "root" will be used.

PASSWORD=root
DB_CONNECTION_NAME=
DB_USER=root
DB_NAME=rdr
DB_INFO_FILE=/tmp/db_info.json
CREATE_DB_FILE=/tmp/create_db.sql

PASSWORD_ARGS="-p${PASSWORD}"
PASSWORD_STRING=":${PASSWORD}"
while true; do
  case "$1" in
    --nopassword) PASSWORD=; PASSWORD_ARGS=; PASSWORD_STRING=; shift 1;;
    --db_user) DB_USER=$2; shift 2;;
    --db_name) DB_NAME=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ "${MYSQL_ROOT_PASSWORD}" ]
then
  PASSWORD="${MYSQL_ROOT_PASSWORD}"
  PASSWORD_ARGS='-p"${PASSWORD}"'
  PASSWORD_STRING=":${PASSWORD}"
fi

# Export this so Alembic can find it.
export DB_CONNECTION_STRING="mysql+mysqldb://${DB_USER}${PASSWORD_STRING}@localhost/${DB_NAME}"

function finish {
  rm -f ${DB_INFO_FILE}
  rm -f ${CREATE_DB_FILE}
}
trap finish EXIT

echo '{"db_connection_string": "'$DB_CONNECTION_STRING'", ' \
     ' "db_password": "'$PASSWORD'", ' \
     ' "db_connection_name": "", '\
     ' "db_user": "'$DB_USER'", '\
     ' "db_name": "'$DB_NAME'" }' > $DB_INFO_FILE
echo 'DROP DATABASE IF EXISTS '$DB_NAME'; CREATE DATABASE '$DB_NAME > $CREATE_DB_FILE

echo "Creating empty database..."
set -x
mysql -u "$DB_USER" $PASSWORD_ARGS < ${CREATE_DB_FILE}
if [ $? != '0' ]
then
  echo "Error creating database. Exiting."
  exit 1
fi

echo "Updating schema to latest..."
tools/upgrade_database.sh

echo "Setting database configuration..."
tools/install_config.sh --key db_config --config ${DB_INFO_FILE} --update


