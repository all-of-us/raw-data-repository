import logging
import yaml
from main_util import configure_logging, get_parser


def main(args):
  base_file = args.base_yaml
  env_file = args.env_yaml
  try:
    with open(base_file, 'r') as base_reader, open(env_file, 'r') as env_reader:
      print yaml.load(base_reader)
      base_lines = base_reader.readlines()
      env_lines = env_reader.readlines()
      # print base_lines, '< base lines'
      # print env_lines, '< env lines'
  except IOError:
    logging.warning('Unable to open files for reading.')



if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--base_yaml', help='The base yaml file.',
                      required=True)
  parser.add_argument('--env_yaml', help='The environment specific yaml file.',
                      required=True)
  main(parser.parse_args())
