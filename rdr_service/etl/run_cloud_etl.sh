# Runs the ETL SQL on a Cloud database.

USAGE="etl/run_cloud_etl.sh --project <PROJECT> --account <ACCOUNT> [--noclone] [--cutoff <2022-04-01>] --vocabulary <vocabulary path>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --cutoff) CUTOFF=$2; shift 2;;
    --vocabulary) VOCABULARY=$2; shift 2;;
    --noclone) NOCLONE="Y"; shift 1;;
    --participant-origin) PARTICIPANT_ORIGIN=$2; shift 2;;
    --participant-list-file) PARTICIPANT_LIST_FILE=$2; shift 2;;
    --include-surveys) INCLUDE_SURVEYS=$2; shift 2;;
    --exclude-surveys) EXCLUDE_SURVEYS=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi

if [ -z "${PROJECT}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi
CREDS_ACCOUNT=${ACCOUNT}

source tools/auth_setup.sh

DATE_WITH_TIME=`date "+%Y%m%d-%H%M%S"`
INSTANCE_NAME=rdr-etl${DATE_WITH_TIME}

INSTANCE_ARGS=""
if [ -z "${NOCLONE}" ]
then
  set -e
  echo "Creating clone instance ${INSTANCE_NAME}..."
  gcloud sql instances clone rdrmaindb ${INSTANCE_NAME}

  # Get the main instance DB info
  get_instance_connection_name
  # Override INSTANCE_CONNECTION_NAME to use the new instance
  INSTANCE_CONNECTION_NAME=${PROJECT}:us-central1:${INSTANCE_NAME}

  function delete_instance {
    set +e
    echo "Deleting instance ${INSTANCE_NAME} (cleanup)..."
    gcloud sql instances delete ${INSTANCE_NAME} --quiet
    set -e
    finish
  }
  trap delete_instance EXIT
  INSTANCE_ARGS="--instance_name ${INSTANCE_NAME}"
else
  get_instance_connection_name
fi

run_cloud_sql_proxy
set_db_connection_string

echo "Running ETL..."

if [ -z "${CUTOFF}" ]
then
  python -m tools curation --project ${PROJECT} cdm-data --vocabulary ${VOCABULARY} --participant-list-file ${PARTICIPANT_LIST_FILE} --exclude-surveys ${EXCLUDE_SURVEYS} --include-surveys ${INCLUDE_SURVEYS} --participant-origin ${PARTICIPANT_ORIGIN}
  mysql -v -v -v -h 127.0.0.1 -u "${ALEMBIC_DB_USER}" -p${PASSWORD} --port ${PORT} < etl/raw_sql/finalize_cdm_data.sql
else
  python -m tools curation --project ${PROJECT} cdm-data --cutoff ${CUTOFF} --vocabulary ${VOCABULARY} --participant-list-file ${PARTICIPANT_LIST_FILE} --exclude-surveys ${EXCLUDE_SURVEYS} --include-surveys ${INCLUDE_SURVEYS} --participant-origin ${PARTICIPANT_ORIGIN}
  sed 's/-- %SED_PM_CUTOFF_FILTER%/AND pm.finalized < "'"${CUTOFF}"'"/g' etl/raw_sql/finalize_cdm_data.sql | mysql -v -v -v -h 127.0.0.1 -u "${ALEMBIC_DB_USER}" -p${PASSWORD} --port ${PORT}
fi

echo "Done with ETL. Please manually run export."
