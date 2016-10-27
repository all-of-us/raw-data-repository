#!/bin/bash

# Runs a hardcoded set of client tests against (by default) the test server.

# Fail if any test fails.
set -e

function usage() {
  echo "Usage: test_server.sh [-i instance] [-r <name match substring>]" >& 2
  exit 1
}

while getopts "i:r:" opt; do
  case $opt in
    i)
      instance=$OPTARG
      echo "Testing ${instance}" >&2
      ;;
    r)
      substring=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
    h|*)
      usage
      ;;
  esac
done

if [ -z "$instance" ];
then
  instance=https://pmi-drc-api-test.appspot.com
  echo "Testing ${instance}"
fi

if [[ $substring ]];
then
   echo Excuting tests that match $substring
fi

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export PYTHONPATH=$PYTHONPATH:${BASE_DIR}:${BASE_DIR}/lib

function run_client_test {
  if [[ $1 == *"$substring"* ]]
  then
    echo Running $1 as it matches substring \"${substring}.\"
    (cd $BASE_DIR/test && PMI_DRC_RDR_INSTANCE=${instance} python $1)
  else
    echo Skipping $1 as it doesn\'t match substring \"${substring}.\"
  fi
}


run_client_test "client_test/ppi.py"
run_client_test "client_test/participant.py"
run_client_test "client_test/evaluation.py"
run_client_test "client_test/metrics.py"
run_client_test "client_test/biobank_order.py"
