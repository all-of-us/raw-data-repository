"""Assigns participants with the specified IDs to the test HPO."""

import csv
import logging

from main_util import get_parser, configure_logging

from client import Client


def main(parser):
  client = Client(parser=parser)
  num_updates = 0
  hpo = client.args.hpo
  with open(client.args.file) as csvfile:
    reader = csv.reader(csvfile)
    for line in reader:
      participant_id = line[0].strip()
      if participant_id:
        client_participant_id = 'P{}'.format(participant_id)
        participant = client.request_json('Participant/{}'.format(client_participant_id))
        if hpo == 'UNSET':
          participant['providerLink'] = []
        else:
          participant['providerLink'] = [{'primary': True,
                                          'organization': {'reference': 'Organization/%s' % hpo}}]
        client.request_json('Participant/{}'.format(client_participant_id), 'PUT', participant,
                            headers={'If-Match': client.last_etag})
        num_updates += 1
  logging.info('Updated %d participants.', num_updates)


if __name__ == '__main__':
  configure_logging()
  arg_parser = get_parser()
  arg_parser.add_argument('--file', help='File containing the list of participant IDs',
                          required=True)
  arg_parser.add_argument('--hpo', help='HPO to assign the participants to; defaults to TEST.',
                          default='TEST')
  main(arg_parser)
