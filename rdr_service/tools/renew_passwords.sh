#!/bin/bash -ae

# Sets up a Cloud SQL instance and sets the 4 passwords that need to be rotated (root, alembic, readonly and rdr


CREATE_INSTANCE=
UPDATE_PASSWORDS=
CONTINUE_CREATING_INSTANCE=
USAGE="tools/renew_passwords.sh --account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    -i) INSTANCE=$2; shift 2;;
    --creds_file) CREDS_FILE=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done
echo "accout $ACCOUNT"
if [ -z "${ACCOUNT}" ]
then
  echo "account: $USAGE"
  exit 1
fi

if [ -z "${PROJECT}" ]
then
  echo "project: $USAGE"
  exit 1
fi

if [ -z "${CREDS_ACCOUNT}" ]
then
  CREDS_ACCOUNT="${ACCOUNT}"
fi

source tools/setup_vars.sh

# Get a randomly generated password including special characters
function randpw {
    new_password=$(< /dev/urandom LC_ALL=C tr -dc 'A-Za-z0-9!@#$%^&*()' < /dev/urandom | head -c 18;echo;)
    }

INSTANCE_NAME=rdrmaindb
FAILOVER_INSTANCE_NAME=rdrbackupdb
# Default to a lightweight config; uses a non-shared CPU, with 1 core and 3.75 GB of memory
# (consider making this something different in production).
MACHINE_TYPE=db-n1-standard-1

source tools/auth_setup.sh

INSTANCE_CONNECTION_NAME=$(gcloud sql instances describe $INSTANCE_NAME | grep connectionName | cut -f2 -d' ')
BACKUP_INSTANCE_NAME=$(gcloud sql instances describe $FAILOVER_INSTANCE_NAME | grep connectionName | cut -f2 -d' ')

if [ ${PROJECT} = 'all-of-us-rdr-sandbox' ]
then
    BACKUP_INSTANCE_NAME=$INSTANCE_CONNECTION_NAME
fi

UPDATE_DB_FILE=/tmp/update_db.sql

function finish {
  cleanup
  #rm -f ${UPDATE_DB_FILE}
}
trap finish EXIT

run_cloud_sql_proxy


echo "Updating database user passwords..."
randpw
ROOT_PASSWORD=$new_password
echo "root pw:"
echo $ROOT_PASSWORD
randpw
RDR_PASSWORD=$new_password
echo "rdr pw:"
echo $RDR_PASSWORD
randpw
READONLY_PASSWORD=$new_password
echo "readonly pw:"
echo $READONLY_PASSWORD

CONNECTION_STRING="mysql+mysqldb://${RDR_DB_USER}:${RDR_PASSWORD}@/$DB_NAME?unix_socket=/cloudsql/$INSTANCE_CONNECTION_NAME&charset=utf8"
BACKUP_CONNECTION_STRING="mysql+mysqldb://${RDR_DB_USER}:${RDR_PASSWORD}@/$DB_NAME?unix_socket=/cloudsql/$BACKUP_INSTANCE_NAME&charset=utf8"

echo '{"db_connection_string": "'$CONNECTION_STRING'", ' \
	     ' "backup_db_connection_string": "'$BACKUP_CONNECTION_STRING'", ' \
	     ' "rdr_db_password": "'$RDR_PASSWORD'", ' \
	     ' "root_db_password": "'$ROOT_PASSWORD'", ' \
	     ' "read_only_db_password": "'$READONLY_PASSWORD'", ' \
	     ' "db_connection_name": "'$INSTANCE_CONNECTION_NAME'", '\
	     ' "backup_db_connection_name": "'$BACKUP_INSTANCE_NAME'", '\
	     ' "db_user": "'$RDR_DB_USER'", '\
	     ' "db_name": "'$DB_NAME'" }' > $TMP_DB_INFO_FILE

echo "Setting root password..."
gcloud sql users set-password root --host % --instance $INSTANCE_NAME --password $ROOT_PASSWORD


echo "Queueing password change commands"
cat tools/update_passwords.sql | envsubst >> $UPDATE_DB_FILE

echo "applying database changes..."
mysql -u "$ROOT_DB_USER" -p"$ROOT_PASSWORD" --host 127.0.0.1 --port ${PORT} < ${UPDATE_DB_FILE} && echo "done" || echo "failed - you will likely need to generate passwords"
echo "Setting database configuration..."

tools/install_config.sh --key db_config --config ${TMP_DB_INFO_FILE} --instance $INSTANCE --update --creds_file ${CREDS_FILE}
echo "Done."

