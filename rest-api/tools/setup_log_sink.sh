#!/bin/bash -e

# Create a Stackdriver logs export (aka sink) to satisfy 180-day logs retention
# for all log types.
# TODO(DA-247) Remove the sinks and use builtin log retention settings once
# they are available.

function usage {
  echo "Usage: $0 --account ${USER}@pmi-ops.org --environment stable"
  exit 1
}

while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --environment) ENV=$2; shift 2;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ]
then
  usage
fi

if [ -z "${ENV}" ]
then
  usage
fi
PROJECT=all-of-us-rdr-${ENV}
BUCKET=${ENV}_logs_archive

LIFECYCLEFILE=$(mktemp --tmpdir lifecycle.json.XXXX)
cat <<EOF > $LIFECYCLEFILE
{
  "rule":
  [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 180}
    }
  ]
}
EOF

gcloud auth login "${ACCOUNT}"
gcloud config set project "${PROJECT}"

echo Creating log sink for $PROJECT in gs://$BUCKET.

gsutil mb "gs://$BUCKET/"
STATUS=$((gcloud beta logging sinks create \
    all-logs-archive  "storage.googleapis.com/${BUCKET}" \
    --log-filter '*') 2>&1)
echo Full status output:
echo $STATUS
echo End full status output
SERVICEACCOUNT=$(echo "${STATUS}" | grep serviceAccount | sed -e 's/.*serviceAccount:\(.*\)`.*/\1/g')
gsutil acl ch -u "${SERVICEACCOUNT}:O" "gs://${BUCKET}/"
gsutil lifecycle set $LIFECYCLEFILE "gs://${BUCKET}/"
rm $LIFECYCLEFILE

echo Sink created, sink account $SERVICEACCOUNT granted access to bucket.
