#!/bin/bash -e
source tools/setup_vars.sh
ROOT_PASSWORD=root
RDR_PASSWORD=rdr!pwd

# Sets DB_CONNECTION_STRING to a connection string used to connect to the database.
# By default, uses the rdr DB user; can be overridden with an argument to this function, e.g.
# set_db_connection_string alembic
# (This works because alembic and rdr users share the same password)
function set_local_db_connection_string {
  DB_USER=$RDR_DB_USER
  if [ "$1" ]
  then
    DB_USER=$1
  fi
  export DB_CONNECTION_STRING="mysql+mysqldb://${DB_USER}:${RDR_PASSWORD}@127.0.0.1/${DB_NAME}?charset=utf8"
}
