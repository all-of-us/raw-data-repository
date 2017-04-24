#!/bin/bash
# Sets ttl=7d for the application-default bucket. This is the bucket used by,
# for example, the metrics pipeline temporary files.
CREATE_INSTANCE=
USAGE="tools/setup_bucket.sh --account <ACCOUNT> --project <PROJECT>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${PROJECT}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi

if [ -z "${CREDS_ACCOUNT}" ]
then
  CREDS_ACCOUNT="${ACCOUNT}"
fi

gcloud auth login $ACCOUNT
gsutil lifecycle set default_bucket_lifecycle.json gs://$PROJECT.appspot.com

