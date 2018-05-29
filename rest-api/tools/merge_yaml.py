from main_util import configure_logging, get_parser

def main(args):
  print args.base_yaml, '< base yaml'
  print args.env_yaml, '< env yaml'


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--base_yaml', help='The base yaml file.',
                      required=True)
  parser.add_argument('--env_yaml', help='The environment specific yaml file.',
                      required=True)
  main(parser.parse_args())
