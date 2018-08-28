#!/bin/bash -e

trap '[[ "$(jobs -p)" ]] && kill $(jobs -p)' EXIT

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
