"""Simple end to end test to exercise the participant and measurements APIs.
"""

import json
import logging

from main_util import configure_logging

from client import Client


def main():
  client = Client()

  request = {
      'facets':['HPO_ID'],
      'start_date': '2017-03-26',
      'end_date': '2017-03-26'
  }

  response = client.request_json('Metrics', 'POST', request)
  logging.info(json.dumps(response, indent=2, sort_keys=True))


if __name__ == '__main__':
  configure_logging()
  main()
