"""Generates fake data on a RDR server.

Creates new participants, creates Biobank samples for existing participants, or both.

Examples:
  generate_fake_data.py --num_participants 10 [--include_*]  # create 10 new participants
  generate_fake_data.py --create_biobank_samples  # store fake sampels CSV for existing participants
"""

import logging

from client import Client
from main_util import get_parser, configure_logging

MAX_PARTICIPANTS_PER_REQUEST = 50


def generate_fake_data(client, args):
  total_participants_created = 0
  while total_participants_created < args.num_participants:
    participants_for_batch = min(MAX_PARTICIPANTS_PER_REQUEST,
                                 args.num_participants - total_participants_created)
    request_body = {'num_participants': participants_for_batch,
                    'include_physical_measurements': args.include_physical_measurements,
                    'include_biobank_orders': args.include_biobank_orders}
    if args.hpo:
      request_body['hpo'] = args.hpo
    logging.info('Generating batch of %d participants.', participants_for_batch)
    client.request_json('DataGen', 'POST', request_body)
    total_participants_created += participants_for_batch
    logging.info('Total participants created: %d', total_participants_created)
  if args.create_biobank_samples:
    logging.info('Requesting Biobank sample generation.')
    client.request_json('DataGen', 'POST', {'create_biobank_samples': True})
    logging.info(
        'Biobank samples are being generated asynchronously.'
        ' Wait until done, then use the cron tab in AppEngine to start the samples pipeline.')
  logging.info('Done.')


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--num_participants',
                      type=int,
                      help='The number of participants to create.',
                      default=0)
  parser.add_argument('--include_physical_measurements',
                      action='store_true',
                      help='True if physical measurements should be created')
  parser.add_argument('--include_biobank_orders',
                      action='store_true',
                      help='True if biobank orders should be created')
  parser.add_argument('--hpo',
                      help='The HPO name to assign participants to; defaults to random choice.')
  parser.add_argument('--create_biobank_samples',
                      action='store_true',
                      help='True if biobank samples should be created')
  rdr_client = Client(parser=parser)
  if rdr_client.args.num_participants == 0 and not rdr_client.args.create_biobank_samples:
    parser.error('--num_participants must be nonzero unless --create_biobank_samples is true.')
  generate_fake_data(rdr_client, rdr_client.args)
