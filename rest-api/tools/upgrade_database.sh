#!/bin/bash

# Applies schema migrations found in alembic/versions to upgrade the database.
# A specific revision level can be provided, or if none is, all revisions will be applied (i.e.
# the schema of the database will be updated to the latest.)

while true; do
  case "$1" in
    --revision) REVISION=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${REVISION}" ]
  then
    REVISION=head
fi

source tools/set_path.sh
(cd ${BASE_DIR}; alembic upgrade ${REVISION})
