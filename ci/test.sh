#!/bin/bash

set -e

trap 'kill $(jobs -p)' EXIT

cd rest-api
pip install -r requirements.txt -t lib/
git submodule update --init

export CLOUDSDK_CORE_DISABLE_PROMPTS=1

# consider adding --clear_datastore=yes
dev_appserver.py . &

until $(curl -s --fail http://localhost:8000); do
    printf '.'
    sleep .25
done

cd ..
./ci/run_command.sh ci/configure_example_user.py

cd rest-api/test
GCLOUD_PATH=$(which gcloud)
SDK_PATH=${GCLOUD_PATH%/bin/gcloud}

./run_tests.sh $SDK_PATH

cd ../../rest-api-client
pip install virtualenv
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

python participant_client.py  --instance http://localhost:8080
python load_fake_participants.py --count 2 --instance http://localhost:8080

