"""Simple end to end test to exercise the participant and evaluation APIs.
"""

import pprint

from client.client import Client

def main():
  client = Client('metrics/v1')

  request = {
      'metric': 'PARTICIPANT_TOTAL',
  }

  response = client.request_json('metrics', 'POST', request)
  pprint.pprint(response)

  request = {
      'metric': 'MEMBERSHIP_TIER',
      'bucket_by': 'WEEK',
  }

  response = client.request_json('metrics', 'POST', request)
  pprint.pprint(response)


if __name__ == '__main__':
  main()
