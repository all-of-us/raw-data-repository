# Tests for the API server

These tests can be run with the run_tests.sh script.

'''Shell
./run_tests.sh ${sdk_dir}
'''

## Directory Structure

The tests are split into two directories:

'unit_test' is for unit_tests.  That is tests that can be run by themselves.

'client_test' are tests that require an instance of the api to be running.