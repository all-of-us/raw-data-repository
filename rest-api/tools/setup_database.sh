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

CREATE_INSTANCE=
UPDATE_PASSWORDS=
USAGE="tools/setup_database.sh --account <ACCOUNT> --project <PROJECT> [--creds_account <ACCOUNT>] [--create_instance | --update_passwords]"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --create_instance) CREATE_INSTANCE=Y; shift 1;;
    --update_passwords) UPDATE_PASSWORDS=Y; shift 1;;
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

if [ -z "${CREDS_ACCOUNT}" ]
then
  CREDS_ACCOUNT="${ACCOUNT}"
fi

source tools/setup_vars.sh

# Prompts the user for a database user password.
function get_required_password {
  db_user_name=$1
  echo -n "Store and share this password in Valentine with"
  echo " name=\"$db_user_name\" and description=\"$PROJECT database\"."
  read -s -p "$db_user_name password for database: " new_password
  echo
  if [ -z "${new_password}" ]
  then
    echo "Password required; exiting."
    exit 1
  fi

  read -s -p "Repeat $db_user_name password: " repeat_new_password
  echo
  if [ "${repeat_new_password}" != "${new_password}" ]
  then
    echo "Password mismatch; exiting."
    exit 1
  fi
}

get_required_password $ROOT_DB_USER
ROOT_PASSWORD=$new_password
get_required_password $RDR_DB_USER/$ALEMBIC_DB_USER
RDR_PASSWORD=$new_password
get_required_password $READONLY_DB_USER
READONLY_PASSWORD=$new_password

INSTANCE_NAME=rdrmaindb
FAILOVER_INSTANCE_NAME=rdrbackupdb
# Default to a lightweight config; uses a non-shared CPU, with 1 core and 3.75 GB of memory
# (consider making this something different in production).
MACHINE_TYPE=db-n1-standard-1

source tools/auth_setup.sh

if [ "${CREATE_INSTANCE}" = "Y" ]
then
  gcloud beta sql instances create $INSTANCE_NAME --tier=$MACHINE_TYPE --activation-policy=ALWAYS \
      --backup-start-time 00:00 --failover-replica-name $FAILOVER_INSTANCE_NAME --enable-bin-log \
      --database-version MYSQL_5_7 --project $PROJECT --storage-auto-increase
  sleep 3
fi
echo "Setting root password..."
gcloud sql instances set-root-password $INSTANCE_NAME --password $ROOT_PASSWORD

INSTANCE_CONNECTION_NAME=$(gcloud sql instances describe $INSTANCE_NAME | grep connectionName | cut -f2 -d' ')
CONNECTION_STRING="mysql+mysqldb://${RDR_DB_USER}:${RDR_PASSWORD}@/$DB_NAME?unix_socket=/cloudsql/$INSTANCE_CONNECTION_NAME&charset=utf8"

UPDATE_DB_FILE=/tmp/update_db.sql

function finish {
  cleanup
  rm -f ${UPDATE_DB_FILE}
}
trap finish EXIT

echo '{"db_connection_string": "'$CONNECTION_STRING'", ' \
     ' "db_password": "'$RDR_PASSWORD'", ' \
     ' "db_connection_name": "'$INSTANCE_CONNECTION_NAME'", '\
     ' "db_user": "'$RDR_DB_USER'", '\
     ' "db_name": "'$DB_NAME'" }' > $TMP_DB_INFO_FILE

for db_name in "rdr" "metrics"; do
  if [ "${UPDATE_PASSWORDS}" = "Y" ]
  then
    cat tools/update_passwords.sql | envsubst > $UPDATE_DB_FILE
  else
    cat tools/create_db.sql | envsubst > $UPDATE_DB_FILE
  fi

  run_cloud_sql_proxy

  if [ "${UPDATE_PASSWORDS}" = "Y" ]
  then
    echo "Updating database user passwords..."
  else
    echo "Creating empty database..."
  fi
  mysql -u "$ROOT_DB_USER" -p"$ROOT_PASSWORD" --host 127.0.0.1 --port ${PORT} < ${UPDATE_DB_FILE}
done

echo "Setting database configuration..."
tools/install_config.sh --key db_config --config ${TMP_DB_INFO_FILE} --instance $INSTANCE --update --creds_file ${CREDS_FILE}

echo "Done."
