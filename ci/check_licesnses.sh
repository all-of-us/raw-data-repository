#!/bin/bash

set -e

# Checks the licenses for all installed python packages.
# This file assumes that it will be exectued from the root pmi-data directory.

# First checks the client libraries in the virtual env.  Assumes that the venv is already set up.
python ./ci/check_licesnses.py --licenses_file ci/license_whitelist.txt

# Next check all the libraries installed in the lib directory for the server.
# Graphy is Apache 2.0 according to it's web page, but it's metadata is lacking.
# dnspython is BSD style, but it's metadata says 'Freeware' which is not a valid license.
PYTHONPATH=rest-api/lib python ./ci/check_licesnses.py \
  --licenses_file ci/license_whitelist.txt --root `pwd` \
  --exceptions 'Graphy,dnspython'
