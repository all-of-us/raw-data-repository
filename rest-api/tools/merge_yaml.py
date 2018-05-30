import logging
import yaml
from pprint import pprint
from main_util import configure_logging, get_parser


def main(args):
  base_file = args.base_yaml
  env_file = args.env_yaml
  try:
    with open(base_file, 'r') as base_reader, open(env_file, 'r') as env_reader:
      base_yaml = yaml.load(base_reader)
      pprint(base_yaml)
      env_yaml = yaml.load(env_reader)
      pprint(env_yaml)
      combined_yaml = (base_yaml.items() + env_yaml.items())
      pprint(combined_yaml)

      with open('app.yaml', 'w') as app:
        app.writelines(yaml.dump(combined_yaml))
        print 'done writing to file'


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
