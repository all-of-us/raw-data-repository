#!/bin/bash -e

trap '[[ "$(jobs -p)" ]] && kill $(jobs -p)' EXIT

cd rdr_client

# Verify that the simplest client script works.
./run_client.sh participant_create_and_get.py
# Verify that data generation works.
./run_client.sh generate_fake_data.py \
    --num_participants 3 \
    --include_physical_measurements --include_biobank_orders --create_biobank_samples
# Verify that we can retrieve awardees successfully.
./run_client.sh get_awardees.py

safety check  # checks current (client) venv

cd ..

cd rest-api/test
GCLOUD_PATH=$(which gcloud)
SDK_PATH=${GCLOUD_PATH%/bin/gcloud}

./run_tests.sh -g $SDK_PATH
