#!/bin/bash -e

PROJ_DIR=`pwd`

trap '[[ "$(jobs -p)" ]] && kill $(jobs -p)' EXIT

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
safety check  # checks current (API) venv

ci/test_pre_push.sh

# Make sure JSON files are well-formed (but don't bother printing them).
for json_file in ./rdr_service/config/*.json; do
    json_pp -t null < $json_file
done

export CLOUDSDK_CORE_DISABLE_PROMPTS=1

python main.py &

sleep 2

cd rdr_service

./tools/install_config.sh --config=config/config_dev.json --update
./tools/setup_local_database.sh --db_user root --db_name circle_test

cd ..

python -m unittest discover -v -s tests
