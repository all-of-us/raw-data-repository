#!/bin/bash -ae

# Sets up a MySQL database named "rdr" locally (dropping the database if it
# already exists), and sets the database config information in the local
# Datastore instance. You must have MySQL installed and running and your local
# dev_appserver instance running before using this.
#
# If you have an environment variable named "MYSQL_ROOT_PASSWORD" it will be
# used as the password to connect to the database; by default, the password
# "root" will be used.
#
# For a fresh database/schema, run this once to set up a blank db, then run
# generate_schema.sh, and then run this again to create that initial schema.

source tools/setup_local_vars.sh
DB_CONNECTION_NAME=
DB_INFO_FILE=/tmp/db_info.json
CREATE_DB_FILE=/tmp/create_db.sql

ROOT_PASSWORD_ARGS="-p${ROOT_PASSWORD}"
while true; do
  case "$1" in
    --nopassword) ROOT_PASSWORD=; ROOT_PASSWORD_ARGS=; shift 1;;
    --db_user) ROOT_DB_USER=$2; shift 2;;
    --db_name) DB_NAME=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ "${MYSQL_ROOT_PASSWORD}" ]
then
  ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}"
  ROOT_PASSWORD_ARGS='-p"${ROOT_PASSWORD}"'
else
  echo "Using a default root mysql password. Set MYSQL_ROOT_PASSWORD to override."
fi

# Set the local db connection string with the RDR user.
set_local_db_connection_string

function finish {
  rm -f ${DB_INFO_FILE}
  rm -f ${CREATE_DB_FILE}
}
trap finish EXIT

echo '{"db_connection_string": "'$DB_CONNECTION_STRING'", ' \
     ' "db_password": "'$RDR_PASSWORD'", ' \
     ' "db_connection_name": "", '\
     ' "db_user": "'$RDR_DB_USER'", '\
     ' "db_name": "'$DB_NAME'" }' > $DB_INFO_FILE
# Include charset here since mysqld defaults to Latin1 (even though CloudSQL
# is configured with UTF8 as the default). Keep in sync with unit_test_util.py.
cat tools/drop_db.sql tools/create_db.sql | envsubst > $CREATE_DB_FILE

echo "Creating empty database..."
mysql -u "$ROOT_DB_USER" $ROOT_PASSWORD_ARGS < ${CREATE_DB_FILE}
if [ $? != '0' ]
then
  echo "Error creating database. Exiting."
  exit 1
fi

# Set it again with the Alembic user for upgrading the database.
set_local_db_connection_string alembic

echo "Updating schema to latest..."
tools/upgrade_database.sh

echo "Setting general configuration..."
tools/install_config.sh --config config/config_dev.json --update

echo "Setting database configuration..."
tools/install_config.sh --key db_config --config ${DB_INFO_FILE} --update

tools/import_data.sh
