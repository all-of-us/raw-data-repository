#!/bin/bash

set -e

trap 'kill $(jobs -p) || true' EXIT

cd rest-api
pip install -r requirements.txt -t lib/
git submodule update --init

export CLOUDSDK_CORE_DISABLE_PROMPTS=1

dev_appserver.py \
  --datastore_path=/tmp/rdr_test_db \
  --clear_datastore=yes \
  . &

until $(curl -s --fail http://localhost:8000); do
    printf '.'
    sleep .25
done

cd ../rest-api-client
pip install virtualenv
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

# The first call will often fail, as it will populate the config store.
# And due to eventual consistency on the config indexes, it often can not be
# used immediately.
# Burn a request to populate the config store.
set +e
python participant_client.py  --instance http://localhost:8080
sleep 2 # Give the indices a chance to get updated.
set -e

cd ..
# The config store needs to be initialized before running this.
./ci/run_command.sh ci/init_config.py

cd rest-api/test
GCLOUD_PATH=$(which gcloud)
SDK_PATH=${GCLOUD_PATH%/bin/gcloud}

./run_tests.sh -g $SDK_PATH

cd ../../rest-api-client
python participant_client.py  --instance http://localhost:8080
python load_fake_participants.py --count 2 --instance http://localhost:8080

