#!/bin/bash

# Runs a hardcoded set of client tests against (by default) the test server.

# Fail if any test fails.
set -e

function usage() {
  echo "Usage: test_server.sh [-i instance] [-r <name match glob>]" >& 2
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
   echo Excuting tests that match glob $substring
fi

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export PYTHONPATH=$PYTHONPATH:${BASE_DIR}:${BASE_DIR}/lib

function run_client_test {
  # Use | instead of / for sed delimiter since we're editing file paths.
  test=`echo $1 | sed -e "s|^$BASE_DIR/test/||"`
  if [[ $test == *"$substring"* ]]
  then
    echo Running $test as it matches substring \"${substring}\".
    (cd $BASE_DIR/test && PMI_DRC_RDR_INSTANCE=${instance} python $test)
  else
    echo Skipping $test as it doesn\'t match substring \"${substring}\".
  fi
}

# Warn if the indicated instance is not local and not https
[[ ${instance} == *localhost* ]] || [[ ${instance} == https://* ]] ||
  echo "WARNING: ${instance} is non-local and not HTTPS; expect failure."

for test in $BASE_DIR/test/client_test/*_test.py
do
  run_client_test "${test}"
done

# Security test: check that HTTPS is required for non-local endpoints.
if [[ ${instance} == https://* ]]
   then
     instance=${instance/https:/http:}
     echo "Checking RDR server at $instance is unreachable over HTTP."
     for test in $BASE_DIR/test/client_test/*_test.py
     do
       ( run_client_test "${test}" 2>&1 | grep "HttpException" ) && echo "OK"
     done
fi

echo "All client tests passed!"
