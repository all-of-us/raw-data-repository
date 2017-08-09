"""Simple client demonstrating how to create and retrieve a participant"""

import logging
import pprint

from main_util import configure_logging

from client import Client


def main():
  client = Client()

  response = client.request_json('Participant', 'POST')
  logging.info(pprint.pformat(response))

  participant_id = response['participantId']
  # Fetch that participant and print it out.
  response = client.request_json('Participant/{}'.format(participant_id))
  logging.info(pprint.pformat(response))


if __name__ == '__main__':
  configure_logging()
  main()
