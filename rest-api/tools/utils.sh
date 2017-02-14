#!/bin/bash

# Utility functions and setup for scripts that need to generate credentials and (optionally)
# run the Cloud SQL proxy
# Expected environment variables: $ACCOUNT, $PROJECT, $CREDS_ACCOUNT, $SERVICE_ACCOUNT

set -e
gcloud auth login $ACCOUNT
gcloud config set project $PROJECT

SERVICE_ACCOUNT="circle-deploy@all-of-us-rdr-staging.iam.gserviceaccount.com"
if [ "${PROJECT}" != "pmi-drc-api-test" ] && [ "${PROJECT}" != "all-of-us-rdr-staging" ]
  then
    SERVICE_ACCOUNT="configurator@${PROJECT}.iam.gserviceaccount.com"
fi

CREDS_FILE=/tmp/creds.json
DB_INFO_FILE=/tmp/db_info.json
PORT=3308
INSTANCE=https://${PROJECT}.appspot.com
CLOUD_PROXY_PID=
PRIVATE_KEY=

function cleanup {
  if [ ! -z $CLOUD_PROXY_PID ];
    then
       kill $CLOUD_PROXY_PID
  fi
  if [ ! -z $PRIVATE_KEY ];
    then
      DELETE_CMD="gcloud iam service-accounts keys delete $PRIVATE_KEY -q --iam-account=$SERVICE_ACCOUNT --account=$CREDS_ACCOUNT" 
      eval $DELETE_CMD
  fi
  rm -f ${CREDS_FILE}
  rm -f ${DB_INFO_FILE}
}

trap cleanup EXIT

CREATE_CREDS_COMMAND="gcloud iam service-accounts keys create $CREDS_FILE --iam-account=$SERVICE_ACCOUNT --account=$CREDS_ACCOUNT"
eval $CREATE_CREDS_COMMAND
PRIVATE_KEY=`grep private_key_id $CREDS_FILE | cut -d\" -f4`

function get_instance_connection_name { 
  echo "Getting database info..."
  tools/install_config.sh --key db_config --instance $INSTANCE --creds_file ${CREDS_FILE} > $DB_INFO_FILE
  INSTANCE_CONNECTION_NAME=`grep db_connection_name $DB_INFO_FILE | cut -d\" -f4`
  echo "INSTANCE = ${INSTANCE_CONNECTION_NAME}"
}

function run_cloud_sql_proxy {
  if [ -z "$INSTANCE_CONNECTION_NAME" ]
    then
      get_instance_connection_name
  fi

  echo "Running cloud proxy..."
  SQL_PROXY_COMMAND="bin/cloud_sql_proxy -instances=${INSTANCE_CONNECTION_NAME}=tcp:${PORT} -credential_file=${CREDS_FILE} &"
  eval $SQL_PROXY_COMMAND                  
  sleep 3
  CLOUD_PROXY_PID=%1
}
