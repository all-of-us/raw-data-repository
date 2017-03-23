"""Generates fake data on a RDR server."""

import argparse

from client.client import Client

MAX_PARTICIPANTS_PER_REQUEST = 100
CREDS_FILE = 'test/test-data/test-client-cert.json'

def main(args):
  client = Client('rdr/v1', False, args.creds_file, args.instance)
  total_participants_created = 0
  while total_participants_created < args.num_participants:
    participants_for_batch = min(MAX_PARTICIPANTS_PER_REQUEST,
                                 args.num_participants - total_participants_created)
    request_body = {'num_participants': participants_for_batch}
    client.request_json('DataGen', 'POST', request_body, test_unauthenticated=False)
    total_participants_created += participants_for_batch
    print "Total participants created: %d" % total_participants_created

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('--instance',
                      type=str,
                      help='The instance to hit, defaults to http://localhost:8080',
                      default='http://localhost:8080')
  parser.add_argument('--creds_file',
                      type=str,
                      help='Path to credentials JSON file.',
                      default=CREDS_FILE)
  parser.add_argument('--num_participants',
                      type=int,
                      help='The number of participants to create.',
                      default=1)

main(parser.parse_args())