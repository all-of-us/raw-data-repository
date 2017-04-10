"""Generates fake data on a RDR server."""

import argparse

from client.client import Client

MAX_PARTICIPANTS_PER_REQUEST = 50
CREDS_FILE = 'test/test-data/test-client-cert.json'

def main(args):
  if args.num_participants == 0 and not args.create_biobank_samples:
    print "Usage: tools/generate_fake_data.py [--num_participants #] [--create_biobank_samples]"
    return
  client = Client('rdr/v1', False, args.creds_file, args.instance)
  total_participants_created = 0
  while total_participants_created < args.num_participants:
    participants_for_batch = min(MAX_PARTICIPANTS_PER_REQUEST,
                                 args.num_participants - total_participants_created)
    request_body = {'num_participants': participants_for_batch,
                    'include_physical_measurements': bool(args.include_physical_measurements),
                    'include_biobank_orders': bool(args.include_biobank_orders)}
    client.request_json('DataGen', 'POST', request_body, test_unauthenticated=False)
    total_participants_created += participants_for_batch
    print "Total participants created: %d" % total_participants_created
  if args.create_biobank_samples:
    request_body = {'create_biobank_samples': True}
    response = client.request_json('DataGen', 'POST', request_body, test_unauthenticated=False)
    print "%d samples generated at %s." % (response['num_samples'],
                                           response['samples_path'])
    if 'localhost' in args.instance:
      print "Starting pipeline..."
      offline_client = Client('offline', False, args.creds_file, args.instance)
      response = offline_client.request_json('BiobankSamplesImport', 'GET', cron=True,
                                  test_unauthenticated=False)
      print "%d samples imported." % response['written']
    else:
      print "Use the cron tab in AppEngine to start the biobank samples pipeline."
  print "Done."

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
                      default=0)
  parser.add_argument('--include_physical_measurements',
                      dest='include_physical_measurements',
                      action='store_true',
                      help='True if physical measurements should be created')
  parser.add_argument('--include_biobank_orders',
                      dest='include_biobank_orders',
                      action='store_true',
                      help='True if biobank orders should be created')
  parser.add_argument('--create_biobank_samples',
                      dest="create_biobank_samples",
                      action='store_true',
                      help='True if biobank samples should be created')

  main(parser.parse_args())
