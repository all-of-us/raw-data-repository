"""Installs a configuration on a RDR server.

See "Config Updates" in the README for usage and permissions details.
"""

import argparse
import difflib
import copy
import json
import httplib

from client.client import Client, HttpException

BASE_CONFIG_FILE = 'config/base_config.json'

def main(args):
  client = Client('rdr/v1', False, args.creds_file, args.instance)
  config_path = 'Config/%s' % args.key if args.key else 'Config'
  try:
    config_server = client.request_json(config_path, 'GET',
                                        test_unauthenticated=False)
    comparable_server = _comparable_string(config_server)
  except HttpException as e:
    if e.code == httplib.NOT_FOUND:
      comparable_server = ''
    else:
      raise

  if not args.config:
    print '----------------- Current Server Config --------------------'
    print comparable_server
  else:
    with open(args.config) as config_file:
      config_file = json.load(config_file)
    if not args.key or args.key == 'current_config':
      with open(BASE_CONFIG_FILE) as base_config_file:
        combined_config = json.load(base_config_file)
      combined_config.update(config_file)
    else:
      combined_config = config_file
    comparable_file = _comparable_string(combined_config)
    configs_match = compare_configs(comparable_file, comparable_server)

    if not configs_match and args.update:
      print '-------------- Updating Server -------------------'
      method = 'POST' if args.key else 'PUT'
      client.request_json(config_path, method, combined_config,
                          test_unauthenticated=False)

def compare_configs(comparable_file, comparable_server):
  if comparable_file == comparable_server:
    print 'Server config matches.'
    return True
  else:
    for line in difflib.context_diff(comparable_server.split('\n'), comparable_file.split('\n')):
      if '"db_connection_string":' in line or '"db_password":' in line:
        print line.split(':')[0] + " *******"
      else:
        print line
  return False

def _comparable_string(config):
  """Sort the values and pretty print so it will compare nicely."""
  config = copy.deepcopy(config)
  for k, v in config.iteritems():
    if isinstance(v, list):
      config[k] = sorted(v)

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
                      help='If this flag is set, then update the remote server',
                      action='store_true')
  parser.add_argument('--creds_file',
                      type=str,
                      help='Path to credentials JSON file.')
  parser.add_argument('--key',
                      type=str,
                      help='Specifies a key for a configuration to update.')
  main(parser.parse_args())
