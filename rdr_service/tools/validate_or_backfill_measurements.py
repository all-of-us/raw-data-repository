"""Tool used to retrieve metadata about all physical measurements in use for participants,
or (when run with --run_backfill) to update all existing physical measurements rows to reflect
all information that can be parsed from the original resources."""

import logging

from pprint import pprint
from dao.physical_measurements_dao import PhysicalMeasurementsDao
from main_util import get_parser, configure_logging


def main(args):
  if args.run_backfill:
    num_updated = PhysicalMeasurementsDao().backfill_measurements()
    logging.info("%d measurements updated." % num_updated)
  else:
    pprint(PhysicalMeasurementsDao().get_distinct_measurements_json(), indent=2)

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--run_backfill', help='Backfill existing physical measurements',
                      action='store_true')

  main(parser.parse_args())