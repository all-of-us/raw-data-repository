#!/bin/bash

# Runs the offline unit tests.  (The ones that don't involve hitting the server)


# Fail if any test fails
set -e

if [ -z "$1" ];
then
  echo "Usage: run_test.sh /path/to/google/cloud/sdk_dir"
  exit 1
fi

if [ -z "$2" ];
then
  subset="all"
else
  subset="$2"
fi

echo Executing $subset

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PYTHONPATH=$PYTHONPATH:${SCRIPT_DIR}/..:${SCRIPT_DIR}/../lib


if [[ "$subset" == "all" || "$subset" == "unit" ]];
then
  # This must be run from the base directory of the appengine app.
  (cd ${SCRIPT_DIR}/..; python test/runner.py --test-path test/unit_test/ $1)
fi


if [[ "$subset" == "all" || "$subset" == "client" ]];
then
  # By default these run against a local dev_server.
  (cd ${SCRIPT_DIR}; python client_test/ppi.py)
  (cd ${SCRIPT_DIR}; python client_test/participant.py)
  (cd ${SCRIPT_DIR}; python client_test/evaluation.py)
  (cd ${SCRIPT_DIR}; python client_test/metrics.py)
  (cd ${SCRIPT_DIR}; python client_test/biobank_order.py)
fi

