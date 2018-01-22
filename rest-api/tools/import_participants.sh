#!/bin/bash -e
. tools/set_path.sh

USAGE="tools/import_participants.sh --file <FILE>  [--account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>]]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --file) FILE=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${FILE}" ]
then
  echo "--file is required. Usage: $USAGE"
  exit 1
fi

if [ -z "${CREDS_ACCOUNT}" ]
then
  CREDS_ACCOUNT="${ACCOUNT}"
fi

EXTRA_ARGS="$@"
EXTRA_ARGS+=" --file ${FILE}"
if [ "${PROJECT}" ]
then
  echo "Getting credentials for ${PROJECT}..."
  source tools/auth_setup.sh
  EXTRA_ARGS=" --creds_file ${CREDS_FILE} --instance ${INSTANCE}"
fi

(cd ${BASE_DIR}; python tools/import_participants.py $EXTRA_ARGS)
