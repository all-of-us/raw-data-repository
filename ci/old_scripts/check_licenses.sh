#!/bin/bash

set -e

# Checks the licenses for all installed python packages.
# This file assumes that it will be exectued from the root repository directory.

# First checks the client libraries in the virtual env.  Assumes that the venv is already set up.
python ./ci/check_licenses.py --licenses_file ci/license_whitelist.txt \
  --exceptions_file ci/license_exceptions.txt

# Next check all the libraries installed in the lib directory for the server.
PYTHONPATH=rest-api/lib python ./ci/check_licenses.py \
  --licenses_file ci/license_whitelist.txt --root `pwd` \
  --exceptions_file ci/license_exceptions.txt
