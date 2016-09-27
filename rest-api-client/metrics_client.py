"""Simple end to end test to exercise the participant and evaluation APIs.
"""

import pprint

import common

def main():
  client = common.Client('metrics/v1')

  total_request = {
      'metric': 'PARTICIPANT_TOTAL',
  }

  response = client.request_json('metrics', 'POST', total_request)
  pprint.pprint(response)


  enrollment_request = {
      'metric': 'PARTICIPANT_MEMBERSHIP_TIER',
  }

  response = client.request_json('metrics', 'POST', enrollment_request)
  pprint.pprint(response)


if __name__ == '__main__':
  main()
