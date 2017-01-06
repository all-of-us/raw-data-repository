#!/bin/bash

set -e

trap 'kill $(jobs -p) || true' EXIT

# No new checked-in credentials.
grep -ril "BEGIN PRIVATE KEY" . | sort > credentials_files
diff credentials_files ci/allowed_private_key_files

cd rest-api

pip install -r requirements.txt -t lib/
git submodule update --init

# Pylint checks
pylint -r n -f text \
  --disable=all \
  --enable=bad-whitespace,unused-import,unused-variable,bad-indentation,broad-except,bare-except,logging-too-many-args \
  *.py \
  offline/*.py \
  client/*.py

# Make sure JSON files are well-formed
for json_file in ./config/*.json; do
    cat $json_file | json_pp;
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
