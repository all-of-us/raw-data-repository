#!/bin/bash -e

# Utility functions and setup for scripts that need to generate credentials and (optionally)
# run the Cloud SQL proxy
# Expected environment variables: $ACCOUNT, $PROJECT, $CREDS_ACCOUNT

TMP_DIR=$(mktemp  -d)

if [ "${CREDS_FILE}" ]
then
  if [ -z "${INSTANCE}" ]
  then
    echo "INSTANCE required if CREDS_FILE specified directly."
    exit 1
  fi
else
  if [ -z "${ACCOUNT}" -o -z "${PROJECT}" -o -z "${CREDS_ACCOUNT}" ]
  then
    echo "ACCOUNT, PROJECT, and CREDS_ACCOUNT required when CREDS_FILE not given."
    exit 1
  fi
fi

if [ "${CREDS_FILE}" ]
then
  echo "Creds file specified directly, skipping gcloud auth."
else
  gcloud auth login $ACCOUNT
  gcloud config set project $PROJECT
  INSTANCE=https://${PROJECT}.appspot.com
  if [ -z "${SERVICE_ACCOUNT}" ]
  then
    SERVICE_ACCOUNT="circle-deploy@all-of-us-rdr-staging.iam.gserviceaccount.com"
    if [ "${PROJECT}" != "pmi-drc-api-test" ] && [ "${PROJECT}" != "all-of-us-rdr-staging" ]
    then
      SERVICE_ACCOUNT="configurator@${PROJECT}.iam.gserviceaccount.com"
    fi
  fi
  CREDS_FILE=${TMP_DIR}/creds.json
  TMP_CREDS_FILE=$CREDS_FILE
  EXISTING_KEYS_COUNT=`gcloud iam service-accounts keys list \
      --iam-account $SERVICE_ACCOUNT --account $CREDS_ACCOUNT | wc -l`
  EXISTING_KEYS_COUNT=`expr $EXISTING_KEYS_COUNT - 1`  # exclude the header.
  echo "Using $CREDS_ACCOUNT to create temporary key for $SERVICE_ACCOUNT in $CREDS_FILE."
  echo "$EXISTING_KEYS_COUNT existing keys for $SERVICE_ACCOUNT."
  gcloud iam service-accounts keys create $CREDS_FILE \
      --iam-account=$SERVICE_ACCOUNT --account=$CREDS_ACCOUNT
  TMP_PRIVATE_KEY=`grep private_key_id $CREDS_FILE | cut -d\" -f4`
fi

REPO_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
source ${REPO_ROOT_DIR}/tools/setup_vars.sh
TMP_GEOCODE_INFO_FILE=${TMP_DIR}/geocode_key.json
TMP_DB_INFO_FILE=${TMP_DIR}/db_info.json
PORT=3308
CLOUD_PROXY_PID=


function cleanup {
  if [ "$CLOUD_PROXY_PID" ];
  then
    kill $CLOUD_PROXY_PID
  fi
  if [ "$TMP_PRIVATE_KEY" ];
  then
    gcloud iam service-accounts keys delete $TMP_PRIVATE_KEY -q \
        --iam-account=$SERVICE_ACCOUNT --account=$CREDS_ACCOUNT
  fi
  if [ "$TMP_GEOCODE_INFO_FILE" ];
  then
    kill $TMP_GEOCODE_INFO_FILE
  fi
  rm -f ${TMP_CREDS_FILE}
  rm -f ${TMP_DB_INFO_FILE}
  rm -rf ${TMP_DIR}
}

trap cleanup EXIT

function get_instance_connection_name {
  echo "Getting database info..."
  tools/install_config.sh --key db_config --instance $INSTANCE \
      --creds_file ${CREDS_FILE} --config_output "$TMP_DB_INFO_FILE"
  INSTANCE_CONNECTION_NAME=`grep db_connection_name $TMP_DB_INFO_FILE | cut -d\" -f4`
}

function get_db_password {
  echo "Getting database password..."
  tools/install_config.sh --key db_config --instance $INSTANCE \
      --creds_file ${CREDS_FILE} --config_output $TMP_DB_INFO_FILE
  PASSWORD=`grep db_password $TMP_DB_INFO_FILE | cut -d\" -f4`
}

function get_geocode_key {
  echo "Getting geocode api key ..."
  tools/install_config.sh --key geocode_key --instance $INSTANCE \
       --config_output "$TMP_GEOCODE_INFO_FILE"
  export API_KEY=$(cat $TMP_GEOCODE_INFO_FILE | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["'api_key'"]')  
}

function run_cloud_sql_proxy {
  if [ -z "$INSTANCE_CONNECTION_NAME" ]
  then
    get_instance_connection_name
  fi

  CLOUD_SQL_PROXY=bin/cloud_sql_proxy
  if [ ! -f "${CLOUD_SQL_PROXY}" ]
  then
    echo "Installing Cloud SQL Proxy at $CLOUD_SQL_PROXY..."
    wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64
    mkdir -p bin
    mv -f cloud_sql_proxy.linux.amd64 "$CLOUD_SQL_PROXY"
    chmod +x "$CLOUD_SQL_PROXY"
  fi

  echo "Running cloud proxy..."
  $CLOUD_SQL_PROXY -instances=${INSTANCE_CONNECTION_NAME}=tcp:${PORT} -credential_file=${CREDS_FILE} &
  sleep 3
  CLOUD_PROXY_PID=%1
}

# Sets DB_CONNECTION_STRING to a connection string used to connect to the database.
# By default, uses the rdr DB user; can be overridden with an argument to this function, e.g.
# set_db_connection_string alembic
# (This works because alembic and rdr users share the same password)
function set_db_connection_string {
  PASSWORD=`grep db_password $TMP_DB_INFO_FILE | cut -d\" -f4`
  DB_USER=$RDR_DB_USER
  if [ $1 ]
  then
    DB_USER=$1
  fi

  function finish {
    cleanup
    export DB_CONNECTION_STRING=
  }
  trap finish EXIT
  export DB_CONNECTION_STRING="mysql+mysqldb://${DB_USER}:${PASSWORD}@127.0.0.1:${PORT}/${DB_NAME}?charset=utf8"
}
