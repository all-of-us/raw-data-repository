#!/bin/bash

set -e

trap 'kill $(jobs -p) || true' EXIT

# No new checked-in credentials.
grep -ril "BEGIN PRIVATE KEY" . | sort > credentials_files
diff credentials_files ci/allowed_private_key_files

function activate_local_venv {
  pip install virtualenv safety
  virtualenv venv
  source venv/bin/activate
  pip install -r requirements.txt
  # The API server doesn't expect to be in venv, it just wants a lib/.
  ln -s venv/lib/python* lib
}

cd rest-api
activate_local_venv
git submodule update --init

# Pylint checks. Use pylint --list-msgs to see more options.
pylint -r n -f text \
  --disable=all \
  --enable=bad-whitespace,bad-indentation,broad-except,bare-except \
  --enable=logging-too-many-args,line-too-long,unused-import,unused-variable \
  --enable=unused-argument,redefined-outer-name,redefined-builtin,unused-import \
  --enable=unused-variable,undefined-variable \
  *.py \
  offline/*.py \
  client/*.py

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

cd ../rest-api-client
activate_local_venv

safety check  # checks current (client) venv

cd ..

./ci/check_licenses.sh

cd rest-api/test
GCLOUD_PATH=$(which gcloud)
SDK_PATH=${GCLOUD_PATH%/bin/gcloud}

./run_tests.sh -g $SDK_PATH

cd ../../rest-api-client
python participant_client.py  --instance http://localhost:8080

python load_fake_participants.py --count 2 --instance http://localhost:8080
