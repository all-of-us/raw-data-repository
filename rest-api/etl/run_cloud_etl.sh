# Runs the ETL SQL on a Cloud database.

USAGE="tools/run_cloud_etl.sh --project <PROJECT> --account <ACCOUNT> [--noclone]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --noclone) NOCLONE="Y"; shift 1;;
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

mysql -v -v -v -h 127.0.0.1 -u "${ALEMBIC_DB_USER}" -p${PASSWORD} --port ${PORT} < etl/etl.sql

echo "Done with ETL. Exporting ETL results..."

./export_etl_results.sh --project ${PROJECT} --account ${ACCOUNT} --directory ${INSTANCE_NAME} ${INSTANCE_ARGS}

if [ -z "${NOCLONE}" ]
then
  SLEEP_TIME=3600
  # TODO: do some kind of polling of task queue to determine when table export has completed.
  echo "Waiting ${SLEEP_TIME} seconds for export to complete."
  sleep ${SLEEP_TIME}
  echo "Sleep done. Instance ${INSTANCE_NAME} will now be deleted."
fi
