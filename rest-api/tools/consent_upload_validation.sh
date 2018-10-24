#!/bin/bash -ae

# Read participant ID's from csv and compare consent docs from ptc-upload-all-of-us-rdr-prod to a given
# source bucket (i.e. aouxxx). Creates a new spreadsheet with discrepancies.

USAGE="tools/consent_upload_validation.sh --input < path to input.csv> --bucket <aouXXX> --account <ACCOUNT> --project <PROJECT>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --input) INPUT=$2; shift 2;;
    --bucket)  BUCKET=$2; shift 2;;
    * ) break ;;
  esac
done

if [ -z "${INPUT}" ] || [ -z "${PROJECT}" ] || [ -z "${ACCOUNT}" ] || [ -z "${BUCKET}" ]
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
gcloud auth activate-service-account $SERVICE_ACCOUNT --key-file ${CREDS_FILE}


echo "Checking files. This may take a while..."
VALIDATE=$(python tools/consent_upload_validation.py --bucket ${BUCKET} --input ${INPUT})

IFS=$'\n';
for message in $VALIDATE;
do
echo ${message}
done
#IFS=${OIFS};

echo "Done!"

EXISTINGFILES=$(find . -maxdepth 1 -ctime -1 -type f -name "existing_files_*" | sed 's|^./||')
MISSINGFILES=$(find . -maxdepth 1 -ctime -1 -type f -name "missing_files_*" | sed 's|^./||')
if [[ "$EXISTINGFILES" ]];then
  echo "Existing consent files ..."
  for f in ${EXISTINGFILES};
  do
    echo "Uploading ${f}"
    gsutil cp ${f} gs://${BUCKET}
  done
else
  echo "No existing consent files found"
fi

if [[ "$MISSINGFILES" ]];then
  echo "Missing consent files ..."
  for f in ${MISSINGFILES};
  do
    echo "Uploading ${f}"
    gsutil cp ${f} gs://${BUCKET}
  done
else
  echo "No missing consent files found"
fi

