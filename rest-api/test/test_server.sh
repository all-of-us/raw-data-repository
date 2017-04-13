#!/bin/bash -e
# Runs all client tests against the local dev server. Fails if any test fails.

function usage() {
  echo "Usage: test_server.sh [-r <name match glob>] [-i <instance URL> -c <creds file>]" >& 2
  exit 1
}

while getopts "i:r:c:" opt; do
  case $opt in
    r)
      substring=$OPTARG
      ;;
    i)
      instance=$OPTARG
      ;;
    c)
      creds_file=$OPTARG
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

if [ "${instance}" ]
then
  if [ -z "${creds_file}" ]
  then
    usage
  fi
else
  instance=http://localhost:8080
fi
echo "Testing ${instance}"

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
    (cd $BASE_DIR/test && \
        PMI_DRC_RDR_INSTANCE=${instance} TESTING_CREDS_FILE=${creds_file} python $test)
  else
    echo Skipping $test as it doesn\'t match substring \"${substring}\".
  fi
}

for test in $BASE_DIR/test/client_test/*_test.py
do
  run_client_test "${test}"
done

echo "All client tests passed!"
