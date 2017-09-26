#!/bin/bash -ae

# Prepares the local databases for running the RDR -> OMOP ETL. 
# Creates "voc" and "cdm" databases alongside "rdr" (run setup_local_database.sh before running
# this); creates tables within them; loads vocabulary files and source_to_concept_map rows into 
# the database. (Note: the loading takes upwards of 10 minutes... grab some coffee.)
#
# Vocabulary CSV files are copied from GCS at path 
# gs://all-of-us-rdr-vocabulary/vocabularies-2017-09-18/; these files were originally received
# via e-mail generated from http://www.ohdsi.org/web/athena/, selecting the vocabularies
# LOINC, SNOMED, AllOFUs_PPI, Race, Gender, and UCUM.
#
# If you have an environment variable named "MYSQL_ROOT_PASSWORD" it will be
# used as the password to connect to the database; by default, the password
# "root" will be used.
#
# Make sure the user you run the script as has write access to /var/lib/mysql-files.

source tools/setup_local_vars.sh
DB_CONNECTION_NAME=
CREATE_DB_FILE=/tmp/create_dbs.sql
CSV_DIR=/var/lib/mysql-files/rdr-csv
CREDS_FILE=/tmp/rdr_creds.json

USAGE="tools/setup_database.sh --account <ACCOUNT> [--db_user <ROOT_DB_USER>] [--nopassword]"
ROOT_PASSWORD_ARGS="-p${ROOT_PASSWORD}"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --nopassword) ROOT_PASSWORD=; ROOT_PASSWORD_ARGS=; shift 1;;
    --db_user) ROOT_DB_USER=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi

if [ "${MYSQL_ROOT_PASSWORD}" ]
then
  ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}"
  ROOT_PASSWORD_ARGS='-p"${ROOT_PASSWORD}"'
else
  echo "Using a default root mysql password. Set MYSQL_ROOT_PASSWORD to override."
fi

# Set the local db connection string with the RDR user.
set_local_db_connection_string

function finish {  
  rm -f ${CREATE_DB_FILE}
  rm -rf ${CSV_DIR}
  rm -f ${CREDS_FILE}
}
trap finish EXIT

# Include charset here since mysqld defaults to Latin1 (even though CloudSQL
# is configured with UTF8 as the default). Keep in sync with unit_test_util.py.
cat etl/create_dbs.sql | envsubst > $CREATE_DB_FILE

echo "Creating voc and cdm databases..."
mysql -h 127.0.0.1 -u "$ROOT_DB_USER" $ROOT_PASSWORD_ARGS < ${CREATE_DB_FILE}
if [ $? != '0' ]
then
  echo "Error creating database. Exiting."
  exit 1
fi

# Set it again with the Alembic user for upgrading the database.
set_local_db_connection_string alembic

mysql -h 127.0.0.1 -u "$ROOT_DB_USER" $ROOT_PASSWORD_ARGS < etl/ddl.sql

# Delete any existing files.
rm -rf ${CSV_DIR}

# Create the directories.
mkdir -p ${CSV_DIR}
mkdir -p ${CSV_DIR}/cdm
mkdir -p ${CSV_DIR}/voc

SERVICE_ACCOUNT=pmi-drc-api-test@appspot.gserviceaccount.com

echo "Activating service account..."
#gcloud iam service-accounts keys create $CREDS_FILE --iam-account=$SERVICE_ACCOUNT --account=$ACCOUNT
#gcloud auth activate-service-account pmi-drc-api-test@appspot.gserviceaccount.com --key-file=$CREDS_FILE

echo "Copying vocabulary files from GCS..."
gsutil cp gs://all-of-us-rdr-vocabulary/vocabularies-2017-09-18/* ${CSV_DIR}/voc

# Rename files to lower case to match table names in schema.
for i in ${CSV_DIR}/voc/*; do mv $i `echo $i | tr [:upper:] [:lower:]`; done

cp etl/source_to_concept_map.csv ${CSV_DIR}/cdm
# Give read permission for MySQL to read the files we're trying to import.
chmod -R 0755 ${CSV_DIR}

echo "Importing source_to_concept_map.csv..."
mysqlimport -u ${ROOT_DB_USER} -p${ROOT_PASSWORD} --fields-terminated-by=\| cdm ${CSV_DIR}/cdm/source_to_concept_map.csv

for file in ${CSV_DIR}/voc/*.csv
do
    echo "Importing ${file}..."    
    mysqlimport -u ${ROOT_DB_USER} -p${ROOT_PASSWORD} --ignore-lines=1 voc ${file}
done

echo "Done."