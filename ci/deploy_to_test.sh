#!/bin/bash

# Deploys the application to the project $1.

# This script is executed by the circle.yml config when the conditions
# specified there are met.

# Requires two environment variables (see below) which combine to decrypt
# the keys for a service account.

# The AppEngine Admin API must be activated for the project being deployed
# to (it is not activated by default).

# The service account must have permissions as described here:
# https://cloud.google.com/appengine/docs/python/access-control

# 1. In particular it requires project editor permission: this is currently
# required to push queues.yaml, cron.yaml, and index.yaml.

# 2. It must also have write access to the two Cloud Storage buckets used
# for staging the deployment - this comes automatically with project editor
# status.

# In the future it will likely be possible to grant weaker permissions
# (such as the AppEngine Deployer role).

set -e

trap 'JOBS_TO_KILL=$(jobs -p) ; if [[ -n $JOBS_TO_KILL ]] ; then kill $JOBS_TO_KILL ; fi' EXIT

PROJECT_ID=$1

echo "Deploying to: $PROJECT_ID"

cd rest-api

export CLOUDSDK_CORE_DISABLE_PROMPTS=1

echo $GCLOUD_CREDENTIALS | \
     openssl enc -d -aes-256-cbc -base64 -A -k $GCLOUD_CREDENTIALS_KEY \
     > ~/gcloud-credentials.key

echo "Deploying RDR to $PROJECT_ID"
gcloud auth activate-service-account --key-file ~/gcloud-credentials.key
gcloud config set project $PROJECT_ID
gcloud app deploy app.yaml cron.yaml index.yaml queue.yaml
echo "RDR deployed to $PROJECT_ID"
