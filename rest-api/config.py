"""Configuration parameters.

Contains things such as the database to connect to.
"""

CLOUDSQL_INSTANCE = 'pmi-drc-api-test:us-central1:pmi-rdr'
CLOUDSQL_SOCKET = '/cloudsql/' + CLOUDSQL_INSTANCE
CLOUDSQL_USER = 'api'


PYTHON_TEST_CLIENT_ID = '116540421226121250670'
ALLOWED_CLIENT_IDS = [PYTHON_TEST_CLIENT_ID]

# TODO: Move all authentication into the datastore.
ALLOWED_USERS = [
    'test-client@pmi-rdr-api-test.iam.gserviceaccount.com',
    'pmi-hpo-staging@appspot.gserviceaccount.com',
]
