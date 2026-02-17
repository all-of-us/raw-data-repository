#!/bin/bash -ae

# Sets up a Cloud SQL instance and sets the 5 passwords that need to be rotated (root, alembic, readonly, rdr, datastream)

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


if [ "${PROJECT}" == "all-of-us-rdr-sandbox" ]
    then DATASTREAM_SECRET_NAME="datastream-rdr-sandbox-password"
elif [ "${PROJECT}" == "all-of-us-rdr-staging" ]
    then DATASTREAM_SECRET_NAME="datastream_password_rdr_warehouse_staging"
elif [ "${PROJECT}" == "all-of-us-rdr-stable" ]
  then DATASTREAM_SECRET_NAME="maindb-datastream-user-password"
elif [ "${PROJECT}" == "all-of-us-rdr-prod" ]
    then DATASTREAM_SECRET_NAME="rdrmaindb-datastream-password"
fi

# Get a randomly generated password including special characters
function randpw {
    lower=$(LC_ALL=C tr -dc 'a-z' < /dev/urandom | head -c1)
    upper=$(LC_ALL=C tr -dc 'A-Z' < /dev/urandom | head -c1)
    digit=$(LC_ALL=C tr -dc '0-9' < /dev/urandom | head -c1)
    special=$(LC_ALL=C tr -dc '!@#$%^&*()' < /dev/urandom | head -c1)
    rest=$(LC_ALL=C tr -dc 'A-Za-z0-9!@#$%^&*()' < /dev/urandom | head -c14)

    new_password="${lower}${upper}${digit}${special}${rest}"
    }

INSTANCE_NAME=rdrmaindb
FAILOVER_INSTANCE_NAME=rdrbackupdb

source tools/auth_setup.sh

INSTANCE_CONNECTION_NAME=$(gcloud sql instances describe $INSTANCE_NAME | grep connectionName | cut -f2 -d' ')
BACKUP_INSTANCE_NAME=$(gcloud sql instances describe $FAILOVER_INSTANCE_NAME | grep connectionName | cut -f2 -d' ')

if [ ${PROJECT} = 'all-of-us-rdr-sandbox' ]
then
    BACKUP_INSTANCE_NAME=$INSTANCE_CONNECTION_NAME
    PORT=3306
fi

UPDATE_DB_FILE=/tmp/update_passwords.sql
UPDATE_DATASTREAM_DB_FILE=/tmp/update_datastream_password.sql

function finish {
  cleanup
  rm -f ${UPDATE_DB_FILE}
  rm -f ${UPDATE_DATASTREAM_DB_FILE}
}
trap finish EXIT

run_cloud_sql_proxy


echo "Updating database user passwords..."
echo "Generating passwords.."
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
if [[ ! -z "$DATASTREAM_SECRET_NAME" ]]; then
  randpw
  DATASTREAM_PASSWORD=$new_password
  echo "datastream pw:"
  echo $DATASTREAM_PASSWORD
fi

CONNECTION_STRING="mysql+mysqldb://${RDR_DB_USER}:${RDR_PASSWORD}@/$DB_NAME?unix_socket=/cloudsql/$INSTANCE_CONNECTION_NAME&charset=utf8"
BACKUP_CONNECTION_STRING="mysql+mysqldb://${RDR_DB_USER}:${RDR_PASSWORD}@/$DB_NAME?unix_socket=/cloudsql/$BACKUP_INSTANCE_NAME&charset=utf8"

echo '{"db_connection_string": "'$CONNECTION_STRING'", ' \
	     ' "backup_db_connection_string": "'$BACKUP_CONNECTION_STRING'", ' \
	     ' "rdr_db_password": "'$RDR_PASSWORD'", ' \
	     ' "root_db_password": "'$ROOT_PASSWORD'", ' \
	     ' "read_only_db_password": "'$READONLY_PASSWORD'", ' \
	     ' "datastream_db_password": "'$DATASTREAM_PASSWORD'", ' \
	     ' "db_connection_name": "'$INSTANCE_CONNECTION_NAME'", '\
	     ' "backup_db_connection_name": "'$BACKUP_INSTANCE_NAME'", '\
	     ' "db_user": "'$RDR_DB_USER'", '\
	     ' "db_name": "'$DB_NAME'" }' > $TMP_DB_INFO_FILE

echo "Setting root password..."
gcloud sql users set-password root --host % --instance $INSTANCE_NAME --password $ROOT_PASSWORD

echo "Queueing password change commands"
cat tools/update_passwords.sql | envsubst >> $UPDATE_DB_FILE

echo "applying database changes..."
if mysql -u "$ROOT_DB_USER" -p"$ROOT_PASSWORD" --host 127.0.0.1 --port ${PORT} < ${UPDATE_DB_FILE}; then
  echo "done"

  echo "Updating Secrets Manager's Root secrets"
  echo -n "$ROOT_PASSWORD" | gcloud secrets versions add rdr-cloud-sql-root-password --data-file=-
  gcloud secrets versions disable $(gcloud secrets versions list rdr-cloud-sql-root-password --sort-by=createTime --format="value(name)" | tail -2 | head -1) --secret=rdr-cloud-sql-root-password

  echo "Updating Secrets Manager's RDR secrets"
  echo -n "$RDR_PASSWORD" | gcloud secrets versions add rdr-cloud-sql-rdr-password --data-file=-
  gcloud secrets versions disable $(gcloud secrets versions list rdr-cloud-sql-rdr-password --sort-by=createTime --format="value(name)" | tail -2 | head -1) --secret=rdr-cloud-sql-rdr-password

  echo "Updating Secrets Manager's READ ONLY secrets"
  echo -n "$READONLY_PASSWORD" | gcloud secrets versions add rdr-cloud-sql-readonly-password --data-file=-
  gcloud secrets versions disable $(gcloud secrets versions list rdr-cloud-sql-readonly-password --sort-by=createTime --format="value(name)" | tail -2 | head -1) --secret=rdr-cloud-sql-readonly-password
else
    echo "failed - you will likely need to generate passwords"
fi

if [[ ! -z "$DATASTREAM_SECRET_NAME" ]]; then
  echo "applying datastream changes..."
  cat tools/update_datastream_password.sql | envsubst >> $UPDATE_DATASTREAM_DB_FILE
  if mysql -u "$ROOT_DB_USER" -p"$ROOT_PASSWORD" --host 127.0.0.1 --port ${PORT} < ${UPDATE_DATASTREAM_DB_FILE}; then
    echo "Updating Secrets Manager's DATASTREAM secrets"
    echo -n "$DATASTREAM_PASSWORD" | gcloud secrets versions add $DATASTREAM_SECRET_NAME --data-file=-
    gcloud secrets versions disable $(gcloud secrets versions list $DATASTREAM_SECRET_NAME --sort-by=createTime --format="value(name)" | tail -2 | head -1) --secret=$DATASTREAM_SECRET_NAME
  else
    echo "failed - you will likely need to regenerate datastream password"
  fi
fi

echo "Secret Manager values updated."

echo "Setting database configuration..."
tools/install_config.sh --key db_config --config ${TMP_DB_INFO_FILE} --instance $INSTANCE --update --creds_file ${CREDS_FILE}
echo "Done."
