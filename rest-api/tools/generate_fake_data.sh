#!/bin/bash

# Set up PYTHONPATH for and call install_config.py.

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export PYTHONPATH=$PYTHONPATH:${BASE_DIR}:${BASE_DIR}/lib

while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${CREDS_ACCOUNT}" ]
then
  CREDS_ACCOUNT="${ACCOUNT}"
fi
EXTRA_ARGS="$@"
if [ "${PROJECT}" ]
then
  echo "Getting credentials for ${PROJECT}..."
  source tools/auth_setup.sh
  EXTRA_ARGS="--creds_file ${CREDS_FILE} --instance ${INSTANCE}"
fi

(cd ${BASE_DIR}; python tools/generate_fake_data.py $EXTRA_ARGS)