import logging
import ruamel.yaml
from collections import defaultdict

import yaml
from pprint import pprint
from main_util import configure_logging, get_parser


def main(args):
  # yaml = ruamel.yaml.YAML()
    base_file = args.base_yaml
    env_file = args.env_yaml
  # try:
  #   with open(base_file, 'r') as base_reader:
  #     base_yaml = yaml.load(base_reader)
  #     with open(env_file, 'r') as env_reader:
  #       env_yaml = yaml.load(env_reader)
  #
  #       for i in env_yaml:
  #         print(i, env_yaml[i])
  #         base_yaml.update({i:env_yaml[i]})
  #
  #       yaml.dump(base_yaml, file('app.yaml'), 'w')
  #       print 'done writing to file'
    sList = []
    for f in (base_file, env_file):
        with open(f, 'r') as stream:
            sList.append(stream.read())
    fString = ''
    for s in sList:
        fString = fString + '\n'+ s

    y = yaml.load(fString)

    yaml.dump(y, file('app.yaml'), 'w+')
    with open('app.yaml', 'w') as app:
      app.writelines(yaml.dump(y))
      print 'done writing to file'
    # return y

  # except IOError:
  #   logging.warning('Unable to open files for reading.')



if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--base_yaml', help='The base yaml file.',
                      required=True)
  parser.add_argument('--env_yaml', help='The environment specific yaml file.',
                      required=True)
  main(parser.parse_args())
