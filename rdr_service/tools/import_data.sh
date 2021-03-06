#!/bin/bash -e
# Imports the codebook, questionnaires, and participants into a non-prod environment.
# Used after setting up a database.
# Expected arguments are --account and --project, passed along to sub-scripts.

source tools/set_path.sh
echo "Importing organizations..."
if [[ ${IS_AUTOMATED_TESTING_ENVIRONMENT} = true ]]
then tools/import_organizations.sh --use_fixture_data $@;
else tools/import_organizations.sh $@;
fi

echo "Importing codebook..."
rdr_client/run_client.sh $@ import_codebook.py
echo "Importing questionnaires..."
tools/import_questionnaires.sh $@
echo "Importing participants..."
tools/import_participants.sh --file $PROJ_DIR/tests/test-data/healthpro_test_participants.csv $@


