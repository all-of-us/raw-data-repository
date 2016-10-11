#!/bin/bash

# Runs the offline unit tests.  (The ones that don't involve hitting the server)


# Fail if any test fails
set -e

if [ -z "$1" ];
then
  echo "Usage: run_test.sh /path/to/google/cloud/sdk_dir"
  exit 1
fi

export PYTHONPATH=$PYTHONPATH:`pwd`/..:../lib
# This must be run from the base directory of the appengine app.
(cd ..; python test/runner.py --test-path test/unit_test/ $1)


# By default these run against a local dev_server.
python client_test/ppi.py
python client_test/participant.py
python client_test/evaluation.py

