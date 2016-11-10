"""Installs a configuration on a local server."""

import argparse
import difflib
import copy
import json

from client.client import Client, HttpException

CREDS_FILE = 'test/test-data/test-client-cert.json'

def main(args):
  client = Client('rdr/v1', False, args.creds_file, args.instance, test_unauthenticated=False)

  config_server = client.request_json('Config', 'GET')
  comparable_server = _comparable_string(config_server)

  if not args.config:
    print '----------------- Current Server Config --------------------'
    print comparable_server
  else:
    with open(args.config) as config_file:
      config_file = json.load(config_file)

    comparable_file = _comparable_string(config_file)
    configs_match = compare_configs(comparable_file, comparable_server)

    if not configs_match and args.update:
      update_server(client, config_server, config_file)

def update_server(client, config_server, config_file):
  print '-------------- Updating Server -------------------'
  client.request_json('Config', 'PUT', config_file)

def compare_configs(comparable_file, comparable_server):
  if comparable_file == comparable_server:
    print 'Server config matches.'
    return True
  else:
    print 'Server config differs.'
    for line in difflib.context_diff(comparable_server.split('\n'), comparable_file.split('\n')):
      print line
  return False

def _comparable_string(config):
  """Sort the values and pretty print so it will compare nicely."""
  config = {k:sorted(v) for k,v in config.iteritems()}
  return json.dumps(config, sort_keys=True, indent=2)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('--config',
                      help='Path to the config.  If omitted, the server config will be printed.')
  parser.add_argument('--instance',
                      type=str,
                      help='The instance to hit, defaults to http://localhost:8080',
                      default='http://localhost:8080')
  parser.add_argument('--update',
                      type=bool,
                      help='If True, will replace the server config.  If False, compares configs.',
                      default=False)
  parser.add_argument('--creds_file',
                      type=str,
                      help='Path to credentials JSON file.',
                      default=CREDS_FILE)
main(parser.parse_args())
