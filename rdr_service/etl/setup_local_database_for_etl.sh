#!/bin/bash -ae

# Prepares the local databases for running the RDR -> OMOP ETL.
# Creates "voc" and "cdm" databases alongside "rdr" (run setup_local_database.sh before running
# this); creates tables within them; loads vocabulary files and source_to_concept_map rows into
# the database. (Note: the loading takes upwards of 10 minutes... grab some coffee.)
#
# Vocabulary CSV files are copied from GCS at path
# gs://all-of-us-rdr-vocabulary/vocabularies-2017-11-27/; these files were originally received
# via e-mail generated from http://www.ohdsi.org/web/athena/, selecting the vocabularies
# LOINC, SNOMED, AllOFUs_PPI, Race, Gender, and UCUM.
#
# If you have an environment variable named "MYSQL_ROOT_PASSWORD" it will be
# used as the password to connect to the database; by default, the password
# "root" will be used.
#
# Make sure the user you run the script as has write access to /var/lib/mysql-files.
# Generates files under /var/lib/mysql-files/cloud-csv (which can be imported into GCS and Cloud
# SQL.)

source tools/setup_local_vars.sh
DB_CONNECTION_NAME=
CSV_DIR=/var/lib/mysql-files/rdr-csv
OUTPUT_DIR=/tmp/rdr-sql-dump

USAGE="etl/setup_local_database_for_etl.sh --account <ACCOUNT> --src-bucket <BUCKET/FOLDER> [--generate_sql_dump <FOLDER>] [--db_user <ROOT_DB_USER>] [--nopassword]"
ROOT_PASSWORD_ARGS="-p${ROOT_PASSWORD}"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --src-bucket) SRCBUCKET=$2; shift 2;;
    --nopassword) ROOT_PASSWORD=; ROOT_PASSWORD_ARGS=; shift 1;;
    --db_user) ROOT_DB_USER=$2; shift 2;;
    --generate_sql_dump) GENERATE_SQL_DUMP="Y"; DSTFOLDER=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ] || [ -z "${SRCBUCKET}" ] ; then
  echo "Usage: $USAGE"
  exit 1
fi

if [ "${MYSQL_ROOT_PASSWORD}" ] && [ -z "${DSTFOLDER}" ]; then
    echo "Usage: $USAGE"
  exit 1
fi

DSTBUCKET="all-of-us-rdr-vocabulary"

echo ""
echo "  source bucket:       ${SRCBUCKET}"
echo "  destination bucket:  ${DSTBUCKET}/${DSTFOLDER}"

echo
while true; do
    read -p "Is this source and destination correct? (y/n) " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer y or n.";;
    esac
done
echo "running..."

if [ "${MYSQL_ROOT_PASSWORD}" ]; then
  ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}"
  ROOT_PASSWORD_ARGS='-p"${ROOT_PASSWORD}"'
else
  echo "Using a default root mysql password. Set MYSQL_ROOT_PASSWORD to override."
fi

# Set the local db connection string with the RDR user.
set_local_db_connection_string

function finish {
  rm -rf ${CSV_DIR}
  rm -rf ${OUTPUT_DIR}
  cleanup
}
trap finish EXIT

echo "Creating voc and cdm databases..."
mysql -h 127.0.0.1 -u "$ROOT_DB_USER" $ROOT_PASSWORD_ARGS < etl/create_dbs.sql
if [ $? != '0' ]
then
  echo "Error creating database. Exiting."
  exit 1
fi

mysql -h 127.0.0.1 -u "$ROOT_DB_USER" $ROOT_PASSWORD_ARGS < etl/ddl.sql
if [ $? != '0' ]
then
  echo "Error creating ETL database. Exiting."
  exit 1
fi

# Delete any existing files.
rm -rf ${CSV_DIR}

# Create the directories.
mkdir -p ${CSV_DIR}
mkdir -p ${CSV_DIR}/cdm
mkdir -p ${CSV_DIR}/voc
mkdir -p ${CSV_DIR}/filters
if [ "${GENERATE_SQL_DUMP}" ]
then
  mkdir -p ${OUTPUT_DIR}
  chmod -R 0777 ${OUTPUT_DIR}
fi

# Create keys for the test service account and activate it to copy the vocabulary files from GCS.
PROJECT=pmi-drc-api-test
CREDS_ACCOUNT="${ACCOUNT}"

source tools/auth_setup.sh

echo "Copying vocabulary files from GCS..."
gsutil cp -r gs://${SRCBUCKET}/CONCEPT.csv ${CSV_DIR}/voc
gsutil cp -r gs://${SRCBUCKET}/CONCEPT_RELATIONSHIP.csv ${CSV_DIR}/voc
echo "Copying filter files from GCS..."
gsutil cp -r gs://${DSTBUCKET}/etl-filters/*.csv ${CSV_DIR}/filters

# Strip concept relationships to "Maps to" as those are the only ones we use in the ETL
grep "Maps to" ${CSV_DIR}/voc/CONCEPT_RELATIONSHIP.csv > ${CSV_DIR}/voc/rel.csv
rm ${CSV_DIR}/voc/CONCEPT_RELATIONSHIP.csv
mv ${CSV_DIR}/voc/rel.csv ${CSV_DIR}/voc/CONCEPT_RELATIONSHIP.csv
cp etl/source_to_concept_map.csv ${CSV_DIR}

# Rename files to lower case to match table names in schema.
for i in ${CSV_DIR}/voc/*; do mv $i `echo $i | tr [:upper:] [:lower:]`; done

# Give read permission for MySQL to read the files we're trying to import.
chmod -R 0777 ${CSV_DIR}

echo "Importing filters..."
mysqlimport -u ${ROOT_DB_USER} $ROOT_PASSWORD_ARGS --local --fields-terminated-by=\| cdm \
    ${CSV_DIR}/filters/combined_question_filter.csv \
    ${CSV_DIR}/filters/combined_survey_filter.csv

echo "Importing source_to_concept_map.csv..."
mysqlimport -u ${ROOT_DB_USER} $ROOT_PASSWORD_ARGS --local --fields-terminated-by=\| cdm ${CSV_DIR}/source_to_concept_map.csv

for file in ${CSV_DIR}/voc/*.csv
do
    echo "Importing ${file}..."
    mysqlimport -u ${ROOT_DB_USER} $ROOT_PASSWORD_ARGS --local --ignore-lines=1 voc ${file}
done

echo "Nulling out empty string fields..."
mysql -v -v -v -h 127.0.0.1 -u "$ROOT_DB_USER" $ROOT_PASSWORD_ARGS < etl/set_empty_strings_to_null.sql

echo "Generating voc intermediate tables..."
mysql -v -v -v -h 127.0.0.1 -u "$ROOT_DB_USER" $ROOT_PASSWORD_ARGS < etl/voc_temp_tables.sql

if [ "${GENERATE_SQL_DUMP}" ]
then
    echo "Generating dump for cdm database.."
    mysqldump --databases cdm -h 127.0.0.1 -u ${ROOT_DB_USER} $ROOT_PASSWORD_ARGS --hex-blob \
      --skip-triggers --default-character-set=utf8 > ${OUTPUT_DIR}/cdm.sql
    echo "Generating dump for voc database.."
    mysqldump --databases voc -h 127.0.0.1 -u ${ROOT_DB_USER} $ROOT_PASSWORD_ARGS --hex-blob \
      --skip-triggers --default-character-set=utf8 > ${OUTPUT_DIR}/voc.sql
    echo "Copying SQL dumps to gs://${DSTBUCKET}/${DSTFOLDER}..."
    gsutil cp -r ${OUTPUT_DIR}/*.sql gs://${DSTBUCKET}/${DSTFOLDER}
fi

echo "Done."
