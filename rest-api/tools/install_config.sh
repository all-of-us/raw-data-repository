#!/bin/bash

# Set up PYTHONPATH for and call install_config.py.

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export PYTHONPATH=$PYTHONPATH:${BASE_DIR}:${BASE_DIR}/lib

while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --update) UPDATE=Y; shift 1;;
    --config) CONFIG=$2; shift 2;;
    --key) KEY=$2; shift 2;;
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
if [ "${CONFIG}" ]
then
  EXTRA_ARGS+=" --config ${CONFIG}"
fi
if [ "${UPDATE}" ]
then
  EXTRA_ARGS+=" --update"
fi
if [ "${KEY}" ]
then
  EXTRA_ARGS+=" --key $KEY"
fi
echo "ARGS = ${EXTRA_ARGS}"

(cd ${BASE_DIR}; python tools/install_config.py $EXTRA_ARGS)
