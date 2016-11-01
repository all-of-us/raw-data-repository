#!/bin/bash

# Manual steps to provision service-account keys for gcloud
# Arg: [path to gcloud service accout key]

set -e

function make_circle_envvar {
    curl -s -X DELETE \
        https://circleci.com/api/v1.1/project/$1/$2/$3/envvar/$4?circle-token=$CI_TOKEN

    curl -s -X POST  --header "Content-Type: application/json" \
        https://circleci.com/api/v1.1/project/$1/$2/$3/envvar?circle-token=$CI_TOKEN \
        -d "{\"name\": \"$4\", \"value\": \"$5\"}"
}

GCLOUD_CREDENTIALS_KEY=$(openssl rand -base64 32)
GCLOUD_CREDENTIALS=$(openssl enc  -aes-256-cbc -in $1 -base64 -A  -k $GCLOUD_CREDENTIALS_KEY)

echo "Environment vars to set in Circle CI Admin UI\n-----\n\n"

echo "GCLOUD_CREDENTIALS=$GCLOUD_CREDENTIALS"
echo "GCLOUD_CREDENTIALS_KEY=$GCLOUD_CREDENTIALS_KEY"

make_circle_envvar github vanderbilt pmi-data GCLOUD_CREDENTIALS $GCLOUD_CREDENTIALS
make_circle_envvar github vanderbilt pmi-data GCLOUD_CREDENTIALS_KEY $GCLOUD_CREDENTIALS_KEY

echo "Created vars":
curl https://circleci.com/api/v1.1/project/github/vanderbilt/pmi-data/envvar?circle-token=$CI_TOKEN

echo Decryption command:
echo $GCLOUD_CREDENTIALS | openssl enc -d -aes-256-cbc -base64 -A -k $GCLOUD_CREDENTIALS_KEY
