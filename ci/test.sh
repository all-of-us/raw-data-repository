#!/bin/bash

set -e

trap 'kill $(jobs -p) || true' EXIT

cd rest-api
pip install -r requirements.txt -t lib/
git submodule update --init

pylint -r n -f text \
  --disable=all \
  --enable=bad-whitespace,unused-import,unused-variable,bad-indentation \
  *.py \
  offline/*.py \
  client/*.py

export CLOUDSDK_CORE_DISABLE_PROMPTS=1

dev_appserver.py \
  --datastore_path=/tmp/rdr_test_db \
  --clear_datastore=yes \
  . &

until $(curl -s --fail http://localhost:8000); do
    printf '.'
    sleep .25
done

./tools/install_config.sh --config=config/config_dev.json --update True

cd ../rest-api-client
pip install virtualenv
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

cd ..

./ci/check_licenses.sh

cd rest-api/test
GCLOUD_PATH=$(which gcloud)
SDK_PATH=${GCLOUD_PATH%/bin/gcloud}

./run_tests.sh -g $SDK_PATH

cd ../../rest-api-client
python participant_client.py  --instance http://localhost:8080
python load_fake_participants.py --count 2 --instance http://localhost:8080

