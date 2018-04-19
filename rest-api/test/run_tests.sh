#!/bin/bash -e
# Runs unit tests (no server interaction) and client tests (hit a local dev
# server). Fails if any test fails.

subset="all"

function usage() {
  echo "Usage: run_test.sh -g /path/to/google/cloud/sdk_dir" \
      "[-s all|unit|client]" \
      "[-r <file name match glob, e.g. 'extraction_*'>]" >& 2
  exit 1
}

while getopts "cs:g:h:r:" opt; do
  case $opt in
    c)
      coverage="true"
      echo "Coverage report generation enabled"
      ;;
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
   echo Excuting tests that match glob $substring
fi

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
. ${BASE_DIR}/tools/set_path.sh

if [[ "$subset" == "all" || "$subset" == "unit" ]];
then
  cmd="test/runner.py --test-path test/unit_test/ ${sdk_dir}"
  if [[ ! -z $substring ]]; then
    cmd="$cmd --test-pattern $substring"
  fi
  if [[ -z $coverage ]]; then
    cmd="$cmd --no-coverage"
  fi
  (cd ${BASE_DIR}; python $cmd)
fi

if [[ "$subset" == "all" || "$subset" == "client" ]];
then
  # Run client tests against local dev_server.
  ${BASE_DIR}/test/test_server.sh ${substring:+-r $substring}
fi
