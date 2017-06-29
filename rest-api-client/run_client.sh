# Gets credentials and runs a Python script that connects to the instance for a project;
# deletes credentials when done.
USAGE="Usage: run_client.sh --project <PROJECT> --account <ACCOUNT> <SCRIPT> [... extra args]

Example: run_client.sh --project pmi-drc-api-test --account dan.rodney@pmi-ops.org participant_test.py
"

API_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../rest-api && pwd )"
source ${API_DIR}/tools/set_path.sh

while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

SCRIPT=$1
shift 1

if [ "${PROJECT}" ]
then
  if [ -z "${ACCOUNT}" ]
  then
   echo "$USAGE"
   exit 1
  fi

  if [ -z "${CREDS_ACCOUNT}" ]
  then
    CREDS_ACCOUNT="${ACCOUNT}"
  fi
  echo "Getting credentials for ${PROJECT}..."
  source ${BASE_DIR}/tools/auth_setup.sh
  echo "Running script..."
  python $SCRIPT --creds_file ${CREDS_FILE} --instance ${INSTANCE} --project ${PROJECT} $@
else
  python $SCRIPT --instance http://localhost:8080 $@
fi




