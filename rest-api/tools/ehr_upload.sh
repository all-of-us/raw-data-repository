#!/bin/bash -ae

# downloads ehr consent per site for an Awardee and uploads it to the Awardees bucket.
# given organization, get list of sites with participant_id's
# get list of participant_id's without site pairing
# for each site, iterate id's and download associated bucket files
# upload participant files to new bucket under a directory named after the site

USAGE="tools/ehr_upload.sh --organization <ORGANIZATION> --source_bucket <BUCKET> --destination_bucket <BUCKET> --account <ACCOUNT> --project <PROJECT>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --organization) ORGANIZATION=$2; shift 2;;
    --source_bucket)  SOURCE_BUCKET=$2; shift 2;;
    --destination_bucket)  DESTINATION_BUCKET=$2; shift 2;;
    * ) break ;;
  esac
done

if [ -z "${ORGANIZATION}" ] || [ -z "${PROJECT}" ] || [ -z "${ACCOUNT}" ] || [ -z "${SOURCE_BUCKET}" ] || [ -z "${DESTINATION_BUCKET}" ]
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
GET_SITES_FOR_ORGANIZATION=$(python tools/ehr_upload.py --organization ${ORGANIZATION} --source_bucket ${SOURCE_BUCKET} --destination_bucket ${DESTINATION_BUCKET})

#GET_SITES_FOR_ORGANIZATION=$(gsutil ls gs://ptc-uploads-all-of-us-rdr-prod)

IFS=$'\n';
for message in $GET_SITES_FOR_ORGANIZATION;
do
echo $message
done
IFS=$OIFS;
