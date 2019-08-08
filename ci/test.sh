#!/bin/bash -e

PROJ_DIR=`pwd`

trap '[[ "$(jobs -p)" ]] && kill $(jobs -p)' EXIT

ci/test_pre_push.sh

python3 -m venv venv3
source venv3/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
safety check  # checks current (API) venv

# Make sure JSON files are well-formed (but don't bother printing them).
for json_file in ./rdr_service/config/*.json; do
    cat $json_file | json_pp -t null;
done

export CLOUDSDK_CORE_DISABLE_PROMPTS=1

python main.py &

until $(curl -s --fail http://localhost:8000); do
    printf '.'
    sleep .25
done

cd rdr_service

./tools/install_config.sh --config=config/config_dev.json --update
./tools/setup_local_database.sh --nopassword --db_user ubuntu --db_name circle_test

cd $PROJ_DIR

./ci/check_licenses.sh

cd rdr_service/rdr_client

# Verify that the simplest client script works.
./run_client.sh participant_create_and_get.py
# Verify that data generation works.
./run_client.sh generate_fake_data.py \
    --num_participants 3 \
    --include_physical_measurements --include_biobank_orders --create_biobank_samples
# Verify that we can retrieve awardees successfully.
./run_client.sh get_awardees.py

cd $PROJ_DIR/rdr_service/test

GCLOUD_PATH=$(which gcloud)
SDK_PATH=${GCLOUD_PATH%/bin/gcloud}

./run_tests.sh -g $SDK_PATH
