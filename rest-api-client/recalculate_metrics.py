"""Simple client call to recalculate metrics on the server."""

import json
import logging

from main_util import configure_logging

from client import Client


def main():
  client = Client(base_path='offline')

  response = client.request_json('MetricsRecalculate', 'GET', cron=True)
  logging.info(json.dumps(response, indent=2, sort_keys=True))


if __name__ == '__main__':
  configure_logging()
  main()
