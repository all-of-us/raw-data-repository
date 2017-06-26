"""Print debugging information about a participant.

Usage: run_client.sh --account $USER@pmi-ops.org --project all-of-us-rdr-prod %(prog)s P12345
"""

import pprint

from client.client import Client
from tools.main_util import configure_logging, get_parser


def get_debug_lines(client, participant_id):
  pprint.pprint(client.request_json('Participant/%s' % participant_id))


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('participant_id', help='P12345 format participant ID to look up.')
  client = Client(parser=parser)
  get_debug_lines(client, client.args.participant_id)
