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
