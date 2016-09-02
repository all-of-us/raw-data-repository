"""Configuration parameters.

Contains things such as the database to connect to.
"""
import endpoints

CLOUDSQL_INSTANCE = 'pmi-drc-api-test:us-central1:pmi-rdr'
CLOUDSQL_SOCKET = '/cloudsql/' + CLOUDSQL_INSTANCE
CLOUDSQL_USER = 'api'


WEB_CLIENT_ID = 'pmi-rdr-test'
PYTHON_TEST_CLIENT_ID = '116540421226121250670'
ALLOWED_CLIENT_IDS = [PYTHON_TEST_CLIENT_ID, WEB_CLIENT_ID,
                      endpoints.API_EXPLORER_CLIENT_ID]
