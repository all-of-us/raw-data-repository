""" Uses a service account to get and sync participant summaries for work queue api
    You can only return results for the same awardee that the service account is connected to.

    Run this using ./run_client.sh --project all-of-us-rdr-prod --account [your account]
    --service_account [service account]@email.com work_queue.py

    You should call code like this with credentials for a service account that has been set up for
    your awardee by AllOfUs.
    Expect to get the same participant summaries back in multiple responses (anything modified
    within 60 seconds of the last summary in the previous response).
    you'll get back relation="next" on the link if there are more results available in the RDR
    that weren't returned and relation="sync" if there were.
    Use the _count parameter to adjust this number. The recommended _count is 5000.

    README: https://github.com/all-of-us/raw-data-repository#participantsummary-api
"""

import logging
import pprint
import time
from main_util import configure_logging
from client import Client


def main():
  client = Client()
  awardee = 'PITT'  # Change to the awardee ID you're requesting data for.
  sync_time = 300  # Number of seconds before next sync.
  response = client.request_json('ParticipantSummary?_count=5000&_sync=true&_sort=lastModified'
                                 '&awardee={}'.format(awardee), 'GET')

  logging.info(pprint.pformat(response))
  sync_url = response['link'][0]['url']
  index = sync_url.find('ParticipantSummary')

  while True:
    time.sleep(sync_time)
    sync_results = sync(client, sync_url, index)
    sync_url = sync_results['link'][0]['url']


def sync(client, sync_url, index):
  print '--------------      Getting next batch from sync...        -----------------', '\n'
  sync_results = client.request_json(sync_url[index:], 'GET')
  logging.info(pprint.pformat(sync_results))
  return sync_results


if __name__ == '__main__':
  configure_logging()
  main()

