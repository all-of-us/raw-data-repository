#!/bin/bash -e
# Use curl to issue requests to RDR API endpoints so a proxy can record them.
#
# This script is single-purpose, for Vanderbilt to record API interactions in
# IBM AppScan and then replay them in a security scan against the PMI test
# environment. It is expected to be run rarely (once to record the interactions,
# and then only again when new interactions need to be recorded) so it's OK to
# require manual setup (getting the bearer token). We also expect the script to
# be run outside of the normal codebase.

# To get the bearer token:
# Generate and save a new service account key (and delete it after you're done).
# Authenticate against it using gcloud (https://cloud.google.com/sdk/downloads):
#     gcloud auth activate-service-account --key-file=<WHEREEVER YOU PUT THAT TOKEN>
#     gcloud auth print-access-token
# Note that during the security scan, AppScan will send a custom header to
# provide its own / new bearer token. (This token need only last for the
# recording phase, and then it can expire after an hour as is normal.)
BEARER_TOKEN=$1

# The HTTP proxy which will record requests.
PROXY=$2

if [ -z "$BEARER_TOKEN" -o -z "$PROXY" ]
then
  echo "Usage: ./make_requests.sh <bearer token> <proxy>"
  exit 1
fi

URL_PREFIX=https://pmi-drc-api-test.appspot.com/rdr/v1

echo Starting requests.
echo Using bearer token $BEARER_TOKEN
echo Using proxy $PROXY

# Endpoints supporting HTTP GET. Note that /Config is excluded since it requries admin permissions.
# Some of these are expected to fail (404) because of invalid IDs.
GET_PATHS=(
    '/'
    '/Participant/P123'
    '/ParticipantSummary'
    '/Questionnaire/456'
    '/Participant/P123/QuestionnaireResponse/456'
    '/Participant/P123/PhysicalMeasurements'
    '/Participant/P123/PhysicalMeasurements/789'
    '/PhysicalMeasurements/_history'
    '/Participant/P123/BiobankOrder/024'
    '/Participant/P123/BiobankOrder'
    '/Participant/P123/BiobankSamples'
    '/MetricsFields'
)

for path in ${GET_PATHS[*]}
do
  full_url=${URL_PREFIX}${path}
  RESPONSE=`mktemp --tmpdir all_of_us_response.XXXXX`
  echo "Requesting $full_url (into $RESPONSE)"
  curl --header "Authorization: Bearer $BEARER_TOKEN" --proxy "$PROXY" --insecure \
      $full_url -o $RESPONSE
  if grep -q Forbidden $RESPONSE
  then
    echo "Authorization failed. Is the token expired, or was the wrong account activated?"
    exit 1
  fi
done

# These POST requests may fail (400) because of invalid request bodies.
POST_PATHS=(
  '/Participant'
  '/Questionnaire'
  '/Participant/P123/QuestionnaireResponse'
  '/Participant/P123/PhysicalMeasurements'
  '/Participant/P123/BiobankOrder'
  '/Metrics'
)
for path in ${POST_PATHS[*]}
do
  full_url=${URL_PREFIX}${path}
  RESPONSE=`mktemp --tmpdir all_of_us_response.XXXXX`
  echo "Requesting $full_url (into $RESPONSE)"
  curl --header "Authorization: Bearer $BEARER_TOKEN" --proxy "$PROXY" --insecure \
      --header "Content-type: application/json" --data '{}' $full_url -o $RESPONSE
done

echo Requests completed successfully, OK to stop recording.
