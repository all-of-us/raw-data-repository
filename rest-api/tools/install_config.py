"""Installs a configuration on a RDR server.

See "Config Updates" in the README for usage and permissions details.
"""

import copy
import difflib
import httplib
import json
import logging

from client import Client, HttpException
from main_util import get_parser, configure_logging

BASE_CONFIG_FILE = 'config/base_config.json'


def _log_and_write_config_lines(raw_config_lines, output_path):
  safe_config_lines = []
  for line in raw_config_lines:
    if '"db_connection_string":' in line or '"db_password":' in line:
      safe_config_lines.append(line.split(':')[0] + ' *******')
    else:
      safe_config_lines.append(line)
  logging.info('\n'.join(safe_config_lines))
  if output_path:
    with open(output_path, 'w') as output_file:
      output_file.write('\n'.join(raw_config_lines))
    logging.info('Unredacted config output written to %r.', output_path)


def main(args):
  client = Client(parse_cli=False, creds_file=args.creds_file, default_instance=args.instance)
  config_path = 'Config/%s' % args.key if args.key else 'Config'
  try:
    config_server = client.request_json(config_path, 'GET')
    formatted_server_config = _json_to_sorted_string(config_server)
  except HttpException as e:
    if e.code == httplib.NOT_FOUND:
      formatted_server_config = ''
    else:
      raise

  if not args.config:
    logging.info('----------------- Current Server Config --------------------')
    _log_and_write_config_lines(formatted_server_config.split('\n'), args.config_output)
  else:
    with open(args.config) as config_file:
      config_file = json.load(config_file)
    if not args.key or args.key == 'current_config':
      with open(BASE_CONFIG_FILE) as base_config_file:
        combined_config = json.load(base_config_file)
      combined_config.update(config_file)
    else:
      combined_config = config_file
    comparable_file = _json_to_sorted_string(combined_config)
    configs_match = _compare_configs(comparable_file, formatted_server_config, args.config_output)

    if not configs_match and args.update:
      logging.info('-------------- Updating Server -------------------')
      method = 'POST' if args.key else 'PUT'
      client.request_json(config_path, method, combined_config)

def _compare_configs(comparable_file, comparable_server, diff_output_path):
  if comparable_file == comparable_server:
    logging.info('Server config matches.')
    return True
  else:
    _log_and_write_config_lines(
        difflib.context_diff(comparable_server.split('\n'), comparable_file.split('\n')),
        diff_output_path)
    return False

def _json_to_sorted_string(config):
  """Sort the values and pretty print so it will compare nicely."""
  config = copy.deepcopy(config)
  for k, v in config.iteritems():
    if isinstance(v, list):
      config[k] = sorted(v)

  return json.dumps(config, sort_keys=True, indent=2)

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
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
  parser.add_argument('--config_output',
                      help='Path to write current config and/or diff into, in addition to logging.')
  main(parser.parse_args())
