#!/bin/bash -e

trap '[[ "$(jobs -p)" ]] && kill $(jobs -p)' EXIT

ci/test_pre_push.sh

function activate_local_venv {
  pip install virtualenv safety
  virtualenv venv
  source venv/bin/activate
  pip install -r requirements.txt
  # The API server doesn't expect to be in venv, it just wants a lib/.
  ln -s venv/lib/python*/site-packages/ lib
  # Install coverage so we can collect test suite metadata
  pip install coverage
}

cd rest-api
activate_local_venv
git submodule update --init

safety check  # checks current (API) venv

# Make sure JSON files are well-formed (but don't bother printing them).
for json_file in ./config/*.json; do
    cat $json_file | json_pp -t null;
done

export CLOUDSDK_CORE_DISABLE_PROMPTS=1

dev_appserver.py \
  --datastore_path=/tmp/rdr_test_db \
  --clear_datastore=yes \
  test.yaml &

until $(curl -s --fail http://localhost:8000); do
    printf '.'
    sleep .25
done

./tools/install_config.sh --config=config/config_dev.json --update
./tools/setup_local_database.sh --nopassword --db_user ubuntu --db_name circle_test

cd ../rdr_client
activate_local_venv

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

./ci/check_licenses.sh

cd rest-api/test
GCLOUD_PATH=$(which gcloud)
SDK_PATH=${GCLOUD_PATH%/bin/gcloud}

./run_tests.sh -g $SDK_PATH
