""" Uses the service account 'awardee-pitt' to get participant summaries for work queue api
    with an awardee of PITT"""

import logging
import pprint

from main_util import configure_logging
from client import Client
from oauth2client.service_account import ServiceAccountCredentials

def main():
  client = Client()
  AWARDEE = 'PITT'
  response = client.request_json('ParticipantSummary?_sync=true&_sort=lastModified&awardee={}'
                                                                      .format(AWARDEE), 'GET')
  logging.info(pprint.pformat(response))
  print '--------------------------------------'
  print dir(client._get_authorized_http().credentials)
  print '--------------------------------------'
  print client._get_authorized_http().__dict__
  print client.creds_file


if __name__ == '__main__':
  configure_logging()
  main()

