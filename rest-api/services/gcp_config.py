#
# # !!! This file is python 3.x compliant !!!
#

import os

# path where temporary service account credential keys are stored
GCP_SERVICE_KEY_STORE = '{0}/.rdr/service-keys'.format(os.path.expanduser('~'))

GCP_PROJECTS = [
  'all-of-us-rdr-prod',
  'all-of-us-rdr-stable',
  'all-of-us-rdr-staging',
  'all-of-us-rdr-sandbox',
  'pmi-drc-api-test'
]

GCP_INSTANCES = {
  'all-of-us-rdr-prod': 'all-of-us-rdr-prod:us-central1:rdrmaindb',
  'all-of-us-rdr-stable': 'all-of-us-rdr-stable:us-central1:rdrmaindb',
  'all-of-us-rdr-staging': 'all-of-us-rdr-staging:us-central1:rdrmaindb',
  'all-of-us-rdr-sandbox': 'all-of-us-rdr-sandbox:us-central1:rdrmaindb',
  'pmi-drc-api-test': 'pmi-drc-api-test:us-central1:rdrmaindb',
}

GCP_REPLICA_INSTANCES = {
  'all-of-us-rdr-prod': 'all-of-us-rdr-prod:us-central1:rdrbackupdb',
  'all-of-us-rdr-stable': 'all-of-us-rdr-stable:us-central1:rdrbackupdb',
  'all-of-us-rdr-staging': 'all-of-us-rdr-staging:us-central1:rdrbackupdb1',
  'all-of-us-rdr-sandbox': 'all-of-us-rdr-sandbox:us-central1:rdrmaindb',
  'pmi-drc-api-test': 'pmi-drc-api-test:us-central1:rdrbackupdb',
}
