#!/bin/bash -ae

# see ehr_upload_for_organization.py for docs

USAGE="tools/ehr_upload.sh --csv_file --indput_bucket  --account <ACCOUNT> --project <PROJECT>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --csv_file) CSV_FILE=$2; shift 2;;
    --input_bucket) INPUT_BUCKET=$2; shift 2;;
    * ) break ;;
  esac
done

if [[ -z "${ACCOUNT}" ]] || [[ -z "${PROJECT}" ]] || [[ -z "${CSV_FILE}" ]] || [[ -z "${INPUT_BUCKET}" ]]
then
  echo "Usage: ${USAGE}"
  exit 1
fi


CREDS_ACCOUNT="${ACCOUNT}"
SERVICE_ACCOUNT="configurator@${PROJECT}.iam.gserviceaccount.com"
source tools/set_path.sh
source tools/auth_setup.sh
run_cloud_sql_proxy
set_db_connection_string
gcloud auth activate-service-account ${SERVICE_ACCOUNT} --key-file ${CREDS_FILE}
GET_SITES_FOR_ORGANIZATION=$(python tools/ehr_upload_for_organization.py --csv_file ${CSV_FILE} --input_bucket ${INPUT_BUCKET})

IFS=$'\n';
for message in $GET_SITES_FOR_ORGANIZATION;
do
echo $message
done
IFS=$OIFS;
