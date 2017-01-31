"""Simple end to end test to exercise the participant and measurements APIs.
"""

import pprint

from client.client import Client

def main():
  client = Client('rdr/v1')

  request = {
      'facets':['HPO_ID'],
  }

  import json

  response = client.request_json('Metrics', 'POST', request)
  print(json.dumps(response, indent=2, sort_keys=True))


if __name__ == '__main__':
  main()
