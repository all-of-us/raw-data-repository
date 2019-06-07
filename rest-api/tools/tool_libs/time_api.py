#! /bin/env python
#
# Template for RDR tool python program.
#

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import collections
import csv
import itertools
import json
import logging
import operator
import sys
import time

import argparse

from services.gcp_utils import gcp_get_app_host_name, gcp_make_auth_header
from tools.tool_libs import GCPProcessContext
from services.system_utils import setup_logging, setup_unicode, make_api_request

_logger = logging.getLogger('rdr_logger')

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = 'time-api'
tool_desc = 'Get timing metrics for a specific set of APIs'


def finite_cycle(iterable, cycle_count):
  for _ in xrange(cycle_count):
    for x in iterable:
      yield x


class ProgramTemplateClass(object):

  request_spec_fields = [
    'path',
  ]
  RequestSpec = collections.namedtuple('RequestSpec', request_spec_fields)

  PathStatus = collections.namedtuple('PathStatus', ['path', 'status'])

  Result = collections.namedtuple('Result', [
    'path_status',
    'total_time',
  ])

  def __init__(self, args):
    self.args = args

    if args:
      self._hostname = gcp_get_app_host_name(self.args.project)
      if self._hostname in ['127.0.0.1', 'localhost']:
        self._host = '{}:{}'.format(self._hostname, 8080)
      else:
        self._host = self._hostname

  def run(self):
    """
    Main program process
    :return: Exit code value
    """
    try:
      specs = list(self._parse_infile(self.args.infile))
      results = map(self.do_request, finite_cycle(specs, self.args.iterations))
      self.write_metrics(results, self.args.outfile)
    except Exception as e:
      _logger.exception(e.message)
      return 1
    else:
      return 0

  def _validate_fieldnames(self, fieldnames):
    if (
      fieldnames is not None
      and len(fieldnames) == len(self.request_spec_fields)
      and all([operator.eq(*x) for x in zip(fieldnames, self.request_spec_fields)])
    ):
      return True
    else:
      raise ValueError("Invalid CSV columns. Expected: {}".format(self.request_spec_fields))

  def _parse_infile(self, infile):
    _logger.info("reading from infile...")
    reader = csv.DictReader(infile)
    self._validate_fieldnames(reader.fieldnames)
    return itertools.imap(lambda row: self.RequestSpec(*row.values()), reader)

  def do_request(self, spec):
    start = time.time()
    code, response = make_api_request(self._host, spec.path, req_type='GET',
                                      headers=gcp_make_auth_header())
    duration = time.time() - start
    if code != 200:
      _logger.warn("{} {} ({} seconds): {}".format(code, spec.path, duration, response))
    else:
      _logger.info("{} {} ({} seconds)".format(code, spec.path, duration))
    return self.Result(self.PathStatus(spec.path, code), duration)

  def write_metrics(self, results, outfile):
    def reduce_result(collector, result):
      key = ':'.join(map(str, result.path_status))
      m = collector.get(key)
      if m is None:
        m = collector[key] = {
        'count': 0,
        'total_time': 0,
        'average_time': 0,
        'max_time': 0,
        'min_time': None,
      }
      m['count'] += 1
      m['total_time'] += result.total_time
      m['average_time'] = m['total_time'] / m['count']
      m['max_time'] = max(result.total_time, m['max_time'])
      if m['min_time'] is None:
        m['min_time'] = result.total_time
      else:
        m['min_time'] = min(result.total_time, m['min_time'])
      return collector
    metrics = reduce(reduce_result, results, {})
    json.dump(metrics, outfile, indent=2)


def run():
  # Set global debug value and setup application logging.
  setup_logging(_logger, tool_cmd,
                '--debug' in sys.argv, '{0}.log'.format(tool_cmd) if '--log-file' in sys.argv else None)
  setup_unicode()

  # Setup program arguments.
  parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
  parser.add_argument('--debug', help='Enable debug output', default=False, action='store_true')  # noqa
  parser.add_argument('--log-file', help='write output to a log file', default=False, action='store_true')  # noqa
  parser.add_argument('--project', help='gcp project name', default='localhost')  # noqa
  parser.add_argument('--account', help='pmi-ops account', default=None)  # noqa
  parser.add_argument('--service-account', help='gcp iam service account', default=None)  # noqa
  parser.add_argument('--infile', '-f', type=argparse.FileType('r'), default=sys.stdin,
                      help='A CSV file of APIs to call (default stdin)')
  parser.add_argument('--outfile', '-o', type=argparse.FileType('w'), default=sys.stdout,
                      help='output target (default stdout)')
  parser.add_argument('--iterations', '-i', type=int, default=1,
                      help='the number of times to hit each request (default 1)')
  args = parser.parse_args()

  with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account):
    process = ProgramTemplateClass(args)
    exit_code = process.run()
    return exit_code


# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
