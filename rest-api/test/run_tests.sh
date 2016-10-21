#!/bin/bash

# Runs the offline unit tests.  (The ones that don't involve hitting the server)


# Fail if any test fails
set -e

subset="all"

function usage() {
  echo "Usage: run_test.sh -g /path/to/google/cloud/sdk_dir [-s all|unit|client] [-r <name match substring>]" >& 2
  exit 1
}

while getopts "s:g:h:r:" opt; do
  case $opt in
    g)
      sdk_dir=$OPTARG
      echo "Using SDK dir: $OPTARG" >&2
      ;;
    s)
      subset=$OPTARG
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

if [ -z "$sdk_dir" ];
then
  usage
fi

if [[ $subset ]];
then
  echo Executing subset $subset
fi


if [[ $substring ]];
then
   echo Excuting tests that match $substring
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PYTHONPATH=$PYTHONPATH:${SCRIPT_DIR}/..:${SCRIPT_DIR}/../lib

if [[ "$subset" == "all" || "$subset" == "unit" ]];
then
  # This must be run from the base directory of the appengine app.
  if [[ -z $substring ]]
  then
    cmd="test/runner.py --test-path test/unit_test/ ${sdk_dir}"
  else
    cmd="test/runner.py --test-path test/unit_test/ ${sdk_dir} --test-pattern $substring"
  fi
  (cd ${SCRIPT_DIR}/..; python $cmd)
fi



function run_client_test {
  if [[ $1 == *"$substring"* ]]
  then
    echo Running $1 as it matches substring \"${substring}.\"
    (cd ${SCRIPT_DIR}; python $1)
  else
    echo Skipping $1 as it doesn\'t match substring \"${substring}.\"
  fi
    
}

if [[ "$subset" == "all" || "$subset" == "client" ]];
then
  # By default these run against a local dev_server.
  run_client_test "client_test/ppi.py"
  run_client_test "client_test/participant.py"
  run_client_test "client_test/evaluation.py"
  run_client_test "client_test/metrics.py"
  run_client_test "client_test/biobank_order.py"
fi
