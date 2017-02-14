#!/bin/bash

# Generates schema migrations in the "alembic/versions" directory.
# Run this before committing whenever you make a change to the model/ directory.

if [ -z "$1" ]
  then
    echo "Usage: tools/generate_schema.sh <MESSAGE>"
    exit 1
fi

source tools/set_path.sh
(cd ${BASE_DIR}; alembic revision --autogenerate -m "$1")
