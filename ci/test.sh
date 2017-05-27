#!/bin/bash -e

trap '[[ "$(jobs -p)" ]] && kill $(jobs -p)' EXIT

ci/test_pre_push.sh

echo "Grepping for checked-in credentials..."
set +e  # OK if grep does not find any matches.
KEY_FILES=`grep -ril "BEGIN PRIVATE KEY" . | grep -v $0 | grep -v oauth2client`
set -e
if [ "${KEY_FILES}" ]
then
  echo "No keys may be checked in, but found: $KEY_FILES"
  exit 1
fi

function activate_local_venv {
  pip install virtualenv safety==1.3.0
  virtualenv venv
  source venv/bin/activate
  pip install -r requirements.txt
  # The API server doesn't expect to be in venv, it just wants a lib/.
  ln -s venv/lib/python*/site-packages/ lib
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


