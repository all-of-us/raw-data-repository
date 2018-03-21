""" Uses the service account 'awardee-pitt' to get participant summaries for work queue api
    with an awardee of PITT"""

import logging
import pprint

from main_util import configure_logging
from client import Client


def main():
  client = Client()
  AWARDEE = 'PITT'
  response = client.request_json('ParticipantSummary?_sync=true&_sort=lastModified&awardee={}'
                                                                      .format(AWARDEE), 'GET')

  sync_url = response['link'][0]['url']
  index = sync_url.find('ParticipantSummary')
  #next_batch = client.request_json(sync_url[index:])
  logging.info(pprint.pformat(response))

if __name__ == '__main__':
  configure_logging()
  main()

