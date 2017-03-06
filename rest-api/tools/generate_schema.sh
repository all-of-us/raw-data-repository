#!/bin/bash

# Generates a schema migration script in the "alembic/versions" directory to
# upgrade the current state of the local MySQL database to the schema declared
# in code. (If alembic/versions/ is empty and MySQL is blank, as after running
# setup_local_database.sh, this will be an initial schema version.)
#
# Run this before committing whenever you make a change to the model/ directory.

if [ -z "$1" ]
then
  echo "Usage: tools/generate_schema.sh <MESSAGE>"
  exit 1
fi

(source tools/set_path.sh; cd ${BASE_DIR}; alembic revision --autogenerate -m "$1")
