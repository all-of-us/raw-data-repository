"""Args parsing code shared with sample clients.
"""

import argparse
import httplib2

from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
CREDS_FILE = '../rest-api/test/test-data/test-client-cert.json'

def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--instance',
      type=str,
      help='The instance to hit, either https://xxx.appspot.com, '
      'or http://localhost:8080',
      default='https://pmi-rdr-api-test.appspot.com')
  return parser.parse_args()


def get_service(api, version, args):
  credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE,
                                                                 [SCOPE])
  http = httplib2.Http()
  http = credentials.authorize(http)

  # Build a service object for interacting with the API.
  discovery_url = '%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (args.instance,
                                                               api, version)
  print discovery_url
  return discovery.build(api, version, discoveryServiceUrl=discovery_url,
                         http=http, cache_discovery=False)
