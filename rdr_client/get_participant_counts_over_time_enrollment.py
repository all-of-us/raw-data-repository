import json
import logging

from main_util import configure_logging

from client import Client


def main():
  client = Client()
  response = client.request_json('ParticipantCountsOverTime?startDate=2019-02-21&endDate=2019-02-21&stratification=ENROLLMENT_STATUS&history=TRUE', 'GET')
  logging.info(json.dumps(response, indent=2, sort_keys=True))


if __name__ == '__main__':
  configure_logging()
  main()
