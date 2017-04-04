#!/bin/bash -e

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

PASSWORD_ARGS="-p${PASSWORD}"
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
else
  echo "Using a default root mysql password. Set MYSQL_ROOT_PASSWORD to override."
fi

# Export this so Alembic can find it.
set_local_db_connection_string

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
# Include charset here since mysqld defaults to Latin1 (even though CloudSQL
# is configured with UTF8 as the default).
cat <<EOSQL > $CREATE_DB_FILE
DROP DATABASE IF EXISTS $DB_NAME;
CREATE DATABASE $DB_NAME CHARACTER SET utf8 COLLATE utf8_general_ci;
EOSQL

echo "Creating empty database..."
mysql -u "$DB_USER" $PASSWORD_ARGS < ${CREATE_DB_FILE}
if [ $? != '0' ]
then
  echo "Error creating database. Exiting."
  exit 1
fi

echo "Updating schema to latest..."
tools/upgrade_database.sh

echo "Setting general configuration..."
tools/install_config.sh --config config/config_dev.json --update

echo "Setting database configuration..."
tools/install_config.sh --key db_config --config ${DB_INFO_FILE} --update

tools/import_data.sh
