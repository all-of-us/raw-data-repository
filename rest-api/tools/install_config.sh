#!/bin/bash

# Installs a config.

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export PYTHONPATH=$PYTHONPATH:${BASE_DIR}:${BASE_DIR}/lib

(cd ${BASE_DIR}; python tools/install_config.py $@)
