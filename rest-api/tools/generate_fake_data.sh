#!/bin/bash -e
. tools/set_path.sh

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
  EXTRA_ARGS="--creds_file ${CREDS_FILE} --instance ${INSTANCE} ${EXTRA_ARGS}"
fi

(cd ${BASE_DIR}; python tools/generate_fake_data.py $EXTRA_ARGS)
