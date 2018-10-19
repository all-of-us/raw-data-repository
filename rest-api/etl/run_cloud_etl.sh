# Runs the ETL SQL on a Cloud database.

USAGE="tools/run_cloud_etl.sh --project <PROJECT> --account <ACCOUNT>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
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

set -e
DATE_WITH_TIME=`date "+%Y%m%d-%H%M%S"`
# Note: this will fail if the instance already exists or was deleted recently. You'll need to
# manually delete the instance and wait a while if that happens.
INSTANCE_NAME=rdr-etl${DATE_WITH_TIME}
echo "Creating clone instance ${INSTANCE_NAME}..."
gcloud sql instances clone rdrmaindb ${INSTANCE_NAME}

# Get the main instance DB info
get_instance_connection_name
# Override INSTANCE_CONNECTION_NAME to use the new instance
INSTANCE_CONNECTION_NAME=${PROJECT}:us-central1:${INSTANCE_NAME}

run_cloud_sql_proxy
set_db_connection_string

function delete_instance {
  set +e
  echo "Deleting instance ${INSTANCE_NAME} (cleanup)..."
  gcloud sql instances delete ${INSTANCE_NAME} --quiet
  set -e
  finish
}

trap delete_instance EXIT


echo "Running ETL..."

mysql -v -v -v -h 127.0.0.1 -u "${ALEMBIC_DB_USER}" -p${PASSWORD} --port ${PORT} < etl/etl.sql

echo "Done with ETL. Exporting ETL results..."

./export_etl_results.sh --project ${PROJECT} --account ${ACCOUNT} --directory ${INSTANCE_NAME} --instance_name ${INSTANCE_NAME}

SLEEP_TIME=3600
# TODO: do some kind of polling of task queue to determine when table export has completed.
echo "Waiting ${SLEEP_TIME} seconds for export to complete."
sleep ${SLEEP_TIME}
