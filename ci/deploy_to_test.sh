#!/bin/bash

set -e

trap 'kill $(jobs -p)' EXIT

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
