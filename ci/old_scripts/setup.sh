#!/bin/bash -e

trap '[[ "$(jobs -p)" ]] && kill $(jobs -p)' EXIT

# Using 'python' command here is referencing
# the global python version 3.7
# This is set in .circleci/config.yml
# because the VM uses pyenv to manage python installations
python -m venv venv3
source venv3/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

export CLOUDSDK_CORE_DISABLE_PROMPTS=1

cd rdr_service
