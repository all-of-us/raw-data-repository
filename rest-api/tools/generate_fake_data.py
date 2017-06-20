"""Generates fake data on a RDR server."""

import logging

from client.client import Client
from tools.main_util import get_parser, configure_logging

MAX_PARTICIPANTS_PER_REQUEST = 50


def main(args):
  if args.num_participants == 0 and not args.create_biobank_samples:
    logging.fatal(
        'Usage: tools/generate_fake_data.py [--num_participants #] [--create_biobank_samples]')
    return
  client = Client(parse_cli=False, creds_file=args.creds_file, default_instance=args.instance)
  total_participants_created = 0
  while total_participants_created < args.num_participants:
    participants_for_batch = min(MAX_PARTICIPANTS_PER_REQUEST,
                                 args.num_participants - total_participants_created)
    request_body = {'num_participants': participants_for_batch,
                    'include_physical_measurements': bool(args.include_physical_measurements),
                    'include_biobank_orders': bool(args.include_biobank_orders)}
    if args.hpo:
      request_body['hpo'] = args.hpo
    client.request_json('DataGen', 'POST', request_body)
    total_participants_created += participants_for_batch
    logging.info('Total participants created: %d', total_participants_created)
  if args.create_biobank_samples:
    request_body = {'create_biobank_samples': 'all'}
    client.request_json('DataGen', 'POST', request_body)
    logging.info(
        'Biobank samples are being generated asynchronously.'
        ' Wait until done, then use the cron tab in AppEngine to start the samples pipeline.')
  logging.info('Done.')


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--instance',
                      type=str,
                      help='The instance to hit, defaults to http://localhost:8080',
                      default='http://localhost:8080')
  parser.add_argument('--creds_file',
                      type=str,
                      help='Path to credentials JSON file.')
  parser.add_argument('--num_participants',
                      type=int,
                      help='The number of participants to create.',
                      default=0)
  parser.add_argument('--include_physical_measurements',
                      dest='include_physical_measurements',
                      action='store_true',
                      help='True if physical measurements should be created')
  parser.add_argument('--include_biobank_orders',
                      dest='include_biobank_orders',
                      action='store_true',
                      help='True if biobank orders should be created')
  parser.add_argument('--hpo',
                      dest='hpo',
                      help='The HPO to assign participants to; defaults to random choice.')
  parser.add_argument('--create_biobank_samples',
                      dest="create_biobank_samples",
                      action='store_true',
                      help='True if biobank samples should be created')

  main(parser.parse_args())
