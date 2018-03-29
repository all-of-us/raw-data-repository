"""Assigns participants with the specified IDs to the organization.

Usage:
  ./run_client.sh --project all-of-us-rdr-prod --account $USER@pmi-ops.org \
      hpo_assigner.py participant_ids_and_hpos.csv --pairing [site|organization|awardee] [--dry_run]
Where site = google_group, organization = external_id, awardee = name.

The CSV contains lines with P12345678,NEW_ORGANIZATION like:
Example awardees:
  P11111111,AZ_TUCSON
  P22222222,AZ_TUCSON
  P99999999,PITT
  P00000000,PITT
"""

import csv
import logging
import sys
from main_util import get_parser, configure_logging

from client import Client, HttpException, client_log


def main(client):
  num_no_change = 0
  num_updates = 0
  num_errors = 0

  if client.args.pairing not in ['site', 'organization', 'awardee']:
    sys.exit('Pairing must be one of site|organization|awardee')

  with open(client.args.file) as csvfile:
    reader = csv.reader(csvfile)
    for line in reader:
      try:
        participant_id, new_pairing = [v.strip() for v in line]
        pairing = client.args.pairing
      except ValueError as e:
        logging.error('Skipping invalid line %d (parsed as %r): %s.', reader.line_num, line, e)
        num_errors += 1
        continue
      if not (new_pairing and participant_id):
        logging.warning(
            'Skipping invalid line %d: missing new_pairing (%r) or participant (%r).',
            reader.line_num, new_pairing, participant_id)
        num_errors += 1
        continue
      if not participant_id.startswith('P'):
        logging.error(
            'Malformed participant ID from line %d: %r does not start with P.',
            reader.line_num, participant_id)
        num_errors += 1
        continue

      try:
        participant = client.request_json('Participant/%s' % participant_id)
      except HttpException as e:
        logging.error('Skipping %s: %s', participant_id, e)
        num_errors += 1
        continue

      old_pairing = _get_old_pairing(participant, pairing)
      if new_pairing == old_pairing:
        num_no_change += 1
        logging.info('%s unchanged (already %s)', participant_id, old_pairing)
        continue

      logging.info('%s %s => %s', participant_id, old_pairing, new_pairing)
      if new_pairing == 'UNSET':
        participant['providerLink'] = []
      else:
        participant[pairing] = new_pairing

      if client.args.dry_run:
        logging.info('Dry run, would update participant[%r] to %r.', pairing, new_pairing)
      else:
        client.request_json('Participant/%s' % participant_id, 'PUT', participant,
                            headers={'If-Match': client.last_etag})
      num_updates += 1
  logging.info(
      '%s %d participants, %d unchanged, %d errors.',
      'Would update' if client.args.dry_run else 'Updated',
      num_updates,
      num_no_change,
      num_errors)


def _get_old_pairing(participant, pairing):
  old_pairing = participant[pairing]
  if not old_pairing:
    return 'UNSET'
  return old_pairing


if __name__ == '__main__':
  configure_logging()
  client_log.setLevel(logging.WARN)  # Suppress the log of HTTP requests.
  arg_parser = get_parser()
  arg_parser.add_argument('file', help='file containing the list of HPOs and participant IDs')
  arg_parser.add_argument('--dry_run', action='store_true')
  arg_parser.add_argument('--pairing', help='set level of pairing as one of'
                          '[site|organization|awardee]', required=True)
  main(Client(parser=arg_parser))
