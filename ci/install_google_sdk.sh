#!/bin/bash

#
# Install the Google Cloud SDK
#
curl -o /tmp/cloud-sdk.tar.gz https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-294.0.0-linux-x86_64.tar.gz
mkdir /tmp/cloud-sdk
tar -xf /tmp/cloud-sdk.tar.gz --directory /tmp/cloud-sdk
cd /tmp/cloud-sdk/google-cloud-sdk
./install.sh -q --additional-components cloud_sql_proxy core gsutil app-engine-python app-engine-python-extras bq
source /tmp/cloud-sdk/google-cloud-sdk/path.bash.inc
gcloud components list
