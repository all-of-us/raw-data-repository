#!/bin/bash -ae

# Sets up a Cloud SQL instance, sets the root password, creates an empty database,
# and sets the database connection info in a database config in Datastore.
#
# Note that this script does *not* set up failover for the database. You can do that manually
# in the environments you want.
#
# creds_account can be provided in cases where the user associated with the service account
# needed to update configuration differs from the account that can update AppEngine for the instance
#
# create_instance can be provided to create the database instance the first time

# Example usage for setting up a database initially:
# tools/setup_database.sh --account dan.rodney@pmi-ops.org --project all-of-us-rdr-staging --create_instance
# Example usage for changing root and rdr/alembic/readonly passwords:
# tools/setup_database.sh --account dan.rodney@pmi-ops.org --project all-of-us-rdr-staging --update_passwords

echo "\n!!! Deprecated Tool !!!  use new style database tool. IE: python -m tools mysql."
echo "whatp3434"
CREATE_INSTANCE=
UPDATE_PASSWORDS=
CONTINUE_CREATING_INSTANCE=
USAGE="tools/r2.sh --account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>]  --update_passwords]"
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
  echo "accUsage: $USAGE"
  exit 1
fi

if [ -z "${PROJECT}" ]
then
  echo "ppUsage: $USAGE"
  exit 1
fi

if [ -z "${CREDS_ACCOUNT}" ]
then
  CREDS_ACCOUNT="${ACCOUNT}"
fi
echo "past ifs"

source tools/setup_vars.sh

# Get a randomly generated password
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
echo "after backup"
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
echo "root pw"
echo $ROOT_PASSWORD
randpw
RDR_PASSWORD=$new_password
echo "rdrpw"
echo $RDR_PASSWORD
randpw
READONLY_PASSWORD=$new_password

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
echo $TMP_DB_INFO_FILE
tools/install_config.sh --key db_config --config ${TMP_DB_INFO_FILE} --instance $INSTANCE --update --creds_file ${CREDS_FILE}
echo "Done."

