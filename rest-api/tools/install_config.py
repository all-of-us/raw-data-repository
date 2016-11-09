"""Installs a configuration on a local server."""

import argparse
import difflib
import copy
import json

from client.client import Client, HttpException

CREDS_FILE = 'test/test-data/test-client-cert.json'

def main(args):
  client = Client('rdr/v1', False, args.creds_file, args.instance)

  config_server = client.request_json('Config', 'GET')
  comparable_server = _comparable_string(config_server)

  if not args.config:
    print '----------------- Current Server Config --------------------'
    print comparable_server
  else:
    with open(args.config) as config_file:
      config_file = json.load(config_file)

    comparable_file = _comparable_string(config_file)
    compare_configs(comparable_file, comparable_server)

    if args.update:
      update_server(client, config_server, config_file)


def update_server(client, config_server, config_file):
  print '-------------- Updating Server -------------------'
  keys_in_server = set(e['key'] for e in config_server)
  keys_in_file = set(e['key'] for e in config_file)
  keys_to_delete = keys_in_server - keys_in_file

  for key in keys_to_delete:
    print 'Deleting {}...'.format(key)
    empty_config = {'values': []}
    client.request_json('Config/{}'.format(key), 'POST', empty_config)

  server_values = {e['key'] : sorted(e['values']) for e in config_server}
  file_values = {e['key'] : sorted(e['values']) for e in config_file}

  for k, v in file_values.iteritems():
    if k in server_values and v == server_values[k]:
      print '{} matches server config...'.format(k)
    else:
      print '{} differs.  Updating...'.format(k)
      client.request_json('Config/{}'.format(k), 'POST', {'values': v})


def compare_configs(comparable_file, comparable_server):
  if comparable_file == comparable_server:
    print 'Server config matches.'
  else:
    print 'Server config differs.'
    for line in difflib.context_diff(comparable_server.split('\n'), comparable_file.split('\n')):
      print line

def _comparable_string(config):
  """Sort the values and pretty print so it will compare nicely."""
  config = copy.deepcopy(config)
  config = sorted(config, key=lambda e: e['key'])
  for entry in config:
    entry['values'] = sorted(entry['values'])
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
