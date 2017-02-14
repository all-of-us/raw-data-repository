#!/bin/bash

# Sets up a MySQL database named "rdr" locally (dropping the database if it already exists),
# and sets the database config information in the 
# local Datastore instance. You must have MySQL installed and running and your local
# dev_appserver instance running before using this.

CONNECTION_STRING=mysql+mysqldb://root:root@localhost/rdr
PASSWORD=root
DB_CONNECTION_NAME=
DB_USER=root
DB_NAME=rdr
DB_INFO_FILE=/tmp/db_info.json
CREATE_DB_FILE=/tmp/create_db.sql

function finish {
  rm -f ${DB_INFO_FILE}
  rm -f ${CREATE_DB_FILE}
}
trap finish EXIT

echo '{"db_connection_string": "'$CONNECTION_STRING'", ' \
     ' "db_password": "'$PASSWORD'", ' \
     ' "db_connection_name": "'$INSTANCE_CONNECTION_NAME'", '\
     ' "db_user": "'$DB_USER'", '\
     ' "db_name": "'$DB_NAME'" }' > $DB_INFO_FILE
echo 'DROP DATABASE IF EXISTS '$DB_NAME'; CREATE DATABASE '$DB_NAME > $CREATE_DB_FILE

echo "Creating empty database..."
MYSQL_COMMAND="mysql -u $DB_USER -p$PASSWORD < ${CREATE_DB_FILE}"
eval $MYSQL_COMMAND

echo "Setting database configuration"
INSTALL_CONFIG_COMMAND="tools/install_config.sh --key db_config --config ${DB_INFO_FILE} --update"
eval $INSTALL_CONFIG_COMMAND

