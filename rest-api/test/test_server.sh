#!/bin/bash -e
# Runs all client tests against a server instance. Fails if any test fails.

function usage() {
  echo "Usage: test_server.sh [-r <name match glob>]"
  echo "  [-i <instance URL> (-c <creds file> | -a <ACCOUNT> -p <PROJECT>) ]" >& 2
  exit 1
}

while getopts "a:p:i:r:c:" opt; do
  case $opt in
    a)
      ACCOUNT=$OPTARG
      ;;
    p)
      PROJECT=$OPTARG
      ;;
    r)
      substring=$OPTARG
      ;;
    i)
      instance=$OPTARG
      ;;
    c)
      CREDS_FILE=$OPTARG
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

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
if [ "${instance}" ]
then
  if [[ -n "${ACCOUNT}" && -n "${PROJECT}" ]]
  then
    echo "Getting credentials for ${PROJECT}..."
    CREDS_ACCOUNT="${ACCOUNT}"
    source ${BASE_DIR}/tools/auth_setup.sh
  elif [[ -z "${CREDS_FILE}" ]]
  then
    echo "If providing -i, must also provide -c or both -a, -p" >& 2
    echo ""
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

. ${BASE_DIR}/tools/set_path.sh

function run_client_test {
  # Use | instead of / for sed delimiter since we're editing file paths.
  test=`echo $1 | sed -e "s|^$BASE_DIR/test/||"`
  if [[ $test == *"$substring"* ]]
  then
    echo Running $test as it matches substring \"${substring}\".
    (cd $BASE_DIR/test && \
        PMI_DRC_RDR_INSTANCE=${instance} TESTING_CREDS_FILE=${CREDS_FILE} python $test)
  else
    echo Skipping $test as it doesn\'t match substring \"${substring}\".
  fi
}

for test in $BASE_DIR/test/client_test/*_test.py
do
  run_client_test "${test}"
done

echo "All client tests passed!"
