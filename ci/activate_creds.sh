#!/bin/bash

# Creates credentials file $1 from two environment variables (see
# below) which combine to decrypt the keys for a service account.
# Does gcloud auth using the result.

set -e

trap 'kill $(jobs -p) || true' EXIT

echo $GCLOUD_CREDENTIALS | \
     openssl enc -d -aes-256-cbc -base64 -A -k $GCLOUD_CREDENTIALS_KEY \
     > $1

gcloud auth activate-service-account --key-file $1

