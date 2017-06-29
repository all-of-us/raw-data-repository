"""Simple client call to recalculate metrics on the server."""

import json

from client.client import Client


def main():
  client = Client(base_path='offline')

  response = client.request_json('MetricsRecalculate', 'GET', cron=True)
  print json.dumps(response, indent=2, sort_keys=True)


if __name__ == '__main__':
  main()
