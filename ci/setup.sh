#!/bin/bash -e

trap '[[ "$(jobs -p)" ]] && kill $(jobs -p)' EXIT

python3 -m venv venv3
source venv3/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

export CLOUDSDK_CORE_DISABLE_PROMPTS=1

cd rdr_service
