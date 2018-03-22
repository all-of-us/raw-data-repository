""" Uses the service account 'awardee-pitt' to get participant summaries for work queue api
    with an awardee of PITT"""

import logging
import pprint

from main_util import configure_logging
from client import Client


def main():
  client = Client()
  AWARDEE = 'PITT'
  response = client.request_json('ParticipantSummary?_sync=true&_count=1&_sort=lastModified&awardee'
                                 '={}'
                                                                      .format(AWARDEE), 'GET')

  sync_url = response['link'][0]['url']
  index = sync_url.find('ParticipantSummary')
  sync_results = client.request_json(sync_url[index:], 'GET')
  logging.info(pprint.pformat(response))
  logging.info(pprint.pformat(sync_results))

if __name__ == '__main__':
  configure_logging()
  main()

