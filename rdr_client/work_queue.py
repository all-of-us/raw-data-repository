""" Uses the service account 'awardee-pitt' to get participant summaries for work queue api
    with an awardee of PITT"""

import logging
import pprint
from main_util import configure_logging
from client import Client


def main():
  client = Client()

  response = client.request_json('ParticipantSummary?_sync=true&_sort=lastModified&awardee=PITT',
                                 'GET')
  logging.info(pprint.pformat(response))

if __name__ == '__main__':
  configure_logging()
  main()

