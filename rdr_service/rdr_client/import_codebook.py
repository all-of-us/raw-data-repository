"""Calls the API which imports the latest version of the codebook."""

import logging
import sys

from client import Client
from main_util import configure_logging, get_parser


def import_codebook(client):
  logging.info('Requesting import of latest codebook in %s.', client.args.project)
  response = client.request_json('ImportCodebook', method='POST')
  logging.info(
      'Published version was v%(published_version)s, now active version is v%(active_version)s.'
      % response)
  success = True
  for error in response.get('error_messages', []):
    logging.error(error)
    success = False
  for status in response.get('status_messages', []):
    logging.info(status)
  return success


if __name__ == '__main__':
  configure_logging()
  if not import_codebook(Client(parser=get_parser())):
    sys.exit(1)
