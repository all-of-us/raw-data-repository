#!/bin/bash -e
source tools/setup_vars.sh
ROOT_PASSWORD=root
RDR_PASSWORD=rdr!pwd

function local_db_connection_string {
  if [[ -z "$1" || -z "$2" ]]
  then
    echo "Usage: local_db_connection_string <db_user> <db_name>" 1>&2
    echo "got local_db_connection_string '${1}' '${2}'" 1>&2
    exit 1
  fi
  echo "mysql+mysqldb://${1}:${RDR_PASSWORD}@127.0.0.1/${2}?charset=utf8"
}

# Sets DB_CONNECTION_STRING and METRICS_DB_CONNECTION_STRING to a connection string used to connect to the database.
# By default, uses the rdr DB user; can be overridden with an argument to this function, e.g.
# set_db_connection_string alembic
# (This works because alembic and rdr users share the same password)
function set_local_db_connection_string {
  DB_USER=$RDR_DB_USER
  if [ "$1" ]
  then
    DB_USER=$1
  fi
  export DB_CONNECTION_STRING="$(local_db_connection_string "${DB_USER}" "${DB_NAME}")"
  export METRICS_DB_CONNECTION_STRING="$(local_db_connection_string "${DB_USER}" "${METRICS_DB_NAME}")"
}
