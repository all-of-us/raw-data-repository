"""Simple end to end test to exercise the participant and evaluation APIs.
"""

import pprint

import common

def main():
  args = common.parse_args()
  service = common.get_service('metrics', 'v1', args)

  total_request = {
      'metric': 'PARTICIPANT_TOTAL',
  }

  response = service.metrics().calculate(
      body=total_request).execute()
  pprint.pprint(response)


  enrollment_request = {
      'metric': 'PARTICIPANT_MEMBERSHIP_TIER',
  }

  response = service.metrics().calculate(
      body=enrollment_request).execute()
  pprint.pprint(response)


if __name__ == '__main__':
  main()
