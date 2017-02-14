#!/bin/bash

# Applies schema migrations found in alembic/versions to upgrade a database.
# A specific revision level can be provided, or if none is, all revisions will be applied (i.e.
# the schema of the database will be updated to the latest.)
# By default, the database running on your local development box will be upgraded; if you specify
# --project and --account, the database for the project will be upgraded.

USAGE="tools/upgrade_database.sh [--revision <REVISION>] [--account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>]]"
while true; do
  case "$1" in
    --revision) REVISION=$2; shift 2;;
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;        
    -- ) shift; break ;;
    * ) break ;;
  esac
done
 
if [ ! -z "${PROJECT}" ]
  then
    if [ -z "${ACCOUNT}" ]
      then
        echo "Usage: $USAGE"
	    exit 1
    fi
    if [ -z "${CREDS_ACCOUNT}" ]
      then
        CREDS_ACCOUNT="${ACCOUNT}"
    fi
fi   
    

if [ -z "${REVISION}" ]
  then
    REVISION=head
fi

DB_USER=root
DB_NAME=rdr

if [ ! -z "${PROJECT}" ]
  then 
    source tools/utils.sh
    run_cloud_sql_proxy
    PASSWORD=`grep db_password $DB_INFO_FILE | cut -d\" -f4`
     function finish {
      cleanup
      export DB_CONNECTION_STRING=
    }
    trap finish EXIT    
    export DB_CONNECTION_STRING="mysql+mysqldb://${DB_USER}:${PASSWORD}@127.0.0.1:${PORT}/${DB_NAME}"       
fi

source tools/set_path.sh
alembic upgrade ${REVISION}

