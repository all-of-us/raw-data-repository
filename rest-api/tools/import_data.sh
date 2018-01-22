#!/bin/bash -e
# Imports the codebook, questionnaires, and participants into a non-prod environment.
# Used after setting up a database.
# Expected arguments are --account and --project, passed along to sub-scripts.

echo "Importing HPOs..."
tools/import_hpos.sh $@
echo "Importing sites..."
tools/import_sites.sh --file test/test-data/sites.csv $@
echo "Importing codebook..."
../rdr_client/run_client.sh $@ import_codebook.py
echo "Importing questionnaires..."
tools/import_questionnaires.sh $@
echo "Importing participants..."
tools/import_participants.sh --file test/test-data/healthpro_test_participants.csv $@

