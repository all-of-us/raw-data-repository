"""Assigns participants with the specified IDs to the test HPO.

Usage:
  ./run_client.sh --project all-of-us-rdr-prod --account $USER@pmi-ops.org \
      hpo_assigner.py participant_ids_and_hpos.csv --pairing [site|organization|awardee] [--dry_run]

Where the CSV contains lines with P12345678,NEW_HPO_ID like:
  P11111111,AZ_TUCSON
  P22222222,AZ_TUCSON
  P99999999,PITT
  P00000000,PITT
"""

import csv
import logging
from main_util import get_parser, configure_logging

from client import Client, HttpException, client_log

_ORG_PREFIX = 'Organization/'


def main(client):
  num_no_change = 0
  num_updates = 0
  num_errors = 0

  with open(client.args.file) as csvfile:
    reader = csv.reader(csvfile)
    for line in reader:
      try:
        participant_id, new_pairing = [v.strip() for v in line]
        pairing = client.args.pairing
        assert pairing in ('site', 'organization', 'awardee')
      except AssertionError:
            logging.error('Pairing must be one of site|organization|awardee')
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

      old_pairing = _get_old_pairing(participant)
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


def _get_old_pairing(participant):
  links = participant['providerLink']
  if not links:
    return 'UNSET'
  org = links[0].get('organization', {}).get('reference')
  if org is None:
    return 'UNSET'
  if not org.startswith(_ORG_PREFIX):
    raise ValueError('Could not parse old organization reference %r.' % org)
  return org[len(_ORG_PREFIX):]


if __name__ == '__main__':
  configure_logging()
  client_log.setLevel(logging.WARN)  # Suppress the log of HTTP requests.
  arg_parser = get_parser()
  arg_parser.add_argument('file', help='file containing the list of HPOs and participant IDs')
  arg_parser.add_argument('--dry_run', action='store_true')
  arg_parser.add_argument('--pairing', help='set level of pairing at site', required=True, default='awardee')
  main(Client(parser=arg_parser))
