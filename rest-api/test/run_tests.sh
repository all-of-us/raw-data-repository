#!/bin/bash

# Runs the offline unit tests.  (The ones that don't involve hitting the server)


# Fail if any test fails
set -e

subset="all"

function usage() {
  echo "Usage: run_test.sh -g /path/to/google/cloud/sdk_dir [-s all|unit|client] [-r <name match substring, e.g. 'extraction_*'>]" >& 2
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

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export PYTHONPATH=$PYTHONPATH:${BASE_DIR}:${BASE_DIR}/lib

if [[ "$subset" == "all" || "$subset" == "unit" ]];
then
  # This must be run from the base directory of the appengine app.
  if [[ -z $substring ]]
  then
    cmd="test/runner.py --test-path test/unit_test/ ${sdk_dir}"
  else
    cmd="test/runner.py --test-path test/unit_test/ ${sdk_dir} --test-pattern $substring"
  fi
  (cd ${BASE_DIR}; python $cmd)
fi

if [[ "$subset" == "all" || "$subset" == "client" ]];
then
  # Run client tests against local dev_server.
  ${BASE_DIR}/test/test_server.sh -i http://localhost:8080 ${substring:+-r $substring}
fi
