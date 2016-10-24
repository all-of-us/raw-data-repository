"""Simple end to end test to exercise the participant and evaluation APIs.
"""

import pprint

from client.client import Client

def main():
  client = Client('rdr/v1')

  request = {
      'metric': 'PARTICIPANT_ZIP_CODE',
      'bucket_by': 'MONTH',
      'start_date': '2016-10-01',
      'end_date': '2017-10-01',
  }

  response = client.request_json('Metrics', 'POST', request)
  pprint.pprint(response)

  request = {
      'metric': 'PARTICIPANT_TOTAL',
  }

  response = client.request_json('Metrics', 'POST', request)
  pprint.pprint(response)

  request = {
      'metric': 'PARTICIPANT_MEMBERSHIP_TIER',
      'bucket_by': 'WEEK',
  }

  response = client.request_json('Metrics', 'POST', request)
  pprint.pprint(response)


if __name__ == '__main__':
  main()
