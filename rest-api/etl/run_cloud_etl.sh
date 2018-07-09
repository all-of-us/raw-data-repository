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

# Set INSTANCE_CONNECTION_NAME to the replica database connection, so we use the replica instead of the
# main DB instance for the ETL.
get_backup_instance_connection_name

echo $INSTANCE_CONNECTION_NAME
run_cloud_sql_proxy
set_db_connection_string

mysql -v -v -v -h 127.0.0.1 -u "${ALEMBIC_DB_USER}" -p${PASSWORD} --port ${PORT} < etl/etl.sql

echo "Done."
