#!/bin/bash -e

# Applies schema migrations found in alembic/versions to upgrade a database.
# A specific revision level can be provided, or if none is, all revisions will be applied (i.e.
# the schema of the database will be updated to the latest.)
#
USAGE="
Examples:
Upgrade the local development database:
    tools/upgrade_database.sh [--revision <REVISION>]
Upgrade a deployed db from your development box:
    tools/upgrade_database.sh --project all-or-us-rdr-staging \
        --account $USER@google.com [--creds_account $USER@pmi-ops.org]
Upgrade a deployed db from CircleCI:
    tools/upgrade_database.sh --instance https://all-of-us-x.appspot.com --creds_file ~/creds_file.key
"

while true; do
  case "$1" in
    --revision) REVISION=$2; shift 2;;
    --account) ACCOUNT=$2; shift 2;;
    --creds_account) CREDS_ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --instance) INSTANCE=$2; shift 2;;
    -i) INSTANCE=$2; shift 2;;
    --creds_file) CREDS_FILE=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ "${PROJECT}" ]
then
  if [ -z "${ACCOUNT}" ]
  then
    echo "--account must be specified with --project. $USAGE"
    exit 1
  fi
  if [ -z "${CREDS_ACCOUNT}" ]
  then
    CREDS_ACCOUNT="${ACCOUNT}"
  fi
elif [ "${INSTANCE}" ]
then
  if [ -z "${CREDS_FILE}" ]
  then
    echo "--creds_file must be specified with --instance. $USAGE"
    exit 1
  fi
fi

if [ -z "${REVISION}" ]
then
  REVISION=heads
fi

if [ "${PROJECT}" -o "${INSTANCE}" ]
then
  source tools/auth_setup.sh
  run_cloud_sql_proxy
  set_db_connection_string alembic
else
  if [ -z ${DB_CONNECTION_STRING} ]
  then
    source tools/setup_local_vars.sh
    set_local_db_connection_string alembic
  fi
fi

# alembic.env.get_url() picks up DB_CONNECTION_STRING to find the db to upgrade.
(source tools/set_path.sh; alembic -c alembic_nph.ini upgrade "${REVISION}")
