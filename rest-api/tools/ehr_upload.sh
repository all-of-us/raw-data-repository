#!/bin/bash -ae

# downloads ehr consent per site for an Awardee and uploads it to the Awardees bucket.
# given awardee, get list of sites with participant_id's
# get list of participant_id's without site pairing
# for each site, iterate id's and download associated bucket files
# upload participant files to new bucket under a directory named after the site

USAGE="tools/ehr_upload.sh --awardee <AWARDEE> --account <ACCOUNT> --project <PROJECT>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --awardee) AWARDEE=$2; shift 2;;
    * ) break ;;
  esac
done

if [ -z "${AWARDEE}" ] || [ -z "${PROJECT}" ] || [ -z "${ACCOUNT}" ]
then
  echo "Usage: ${USAGE}"
  exit 1
fi

CREDS_ACCOUNT="${ACCOUNT}"
#SERVICE_ACCOUNT="exporter@${PROJECT}.iam.gserviceaccount.com"
source tools/set_path.sh
source tools/auth_setup.sh
CONNECTION=get_instnace_connection_name
#gcloud auth activate-service-account --key-file ${CREDS_FILE}

gsutil ls -l gs://ptc-uploads-pmi-drc-api-sandbox/Participant

GET_AWARDEE_INFO=$(python tools/ehr_upload.py --awardee ${AWARDEE})
echo '******************'
echo $GET_AWARDEE_INFO
echo '*******************'
#gsutil cp gs://ptc-uploads-pmi-drc-api-sandbox/Participant/P100/botw_emoji.jpg  gs://ptc-uploads-pmi-drc-api-sandbox/Participant/P200
