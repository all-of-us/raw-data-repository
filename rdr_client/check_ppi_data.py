"""Wrapper script for using the PPI validation API.

Downloads PPI test data from a Google spreadsheet, sends it to the API, and logs the results.

The spreadsheet should have data like:
  ConsentPII_EmailAddress, cabor@example.com, columbiany@example.com, ...
  PIIName_First, Jiwon, Riley, ...
  Race_WhatRaceEthnicity, WhatRaceEthnicity_Hispanic|WhatRaceEthnicity_Black, ...
where the first column is question codes, and each subsequent column is one test participant's
answers. (Multiple values for one answer may be separated by | characters.)

Usage: run_client.sh --account $USER@pmi-ops.org --project all-of-us-rdr-staging check_ppi_data.py \
    <spreadsheet doc ID> <spreadsheet sheet ID (GID)>
    [--email cabor@example.com --email columbiany@example.com]
"""

import csv
import httplib
import logging
import urllib2
import re

from client import Client
from code_constants import EMAIL_QUESTION_CODE as EQC, LOGIN_PHONE_NUMBER_QUESTION_CODE as PNQC
from main_util import get_parser, configure_logging


def check_ppi_data(client, args):
  """
  Fetch and process spreadsheet, then call CheckPpiData for results
  :param client: Client object
  :param args: program arguments
  """
  # See if we have filter criteria
  if not args.email and not args.phone:
    do_filter = False
  else:
    do_filter = True

  if not args.phone:
    args.phone = list()
  if not args.email:
    args.email = list()

  csv_data = _fetch_csv_data(args.spreadsheet_id, args.spreadsheet_gid)
  ppi_data = dict()

  # iterate over each data column, convert them into a dict.
  for column in range(0, len(csv_data[0]) - 1):
    row_dict = _convert_csv_column_to_dict(csv_data, column)
    email = row_dict[EQC] if EQC in row_dict else None
    phone_no = row_dict[PNQC] if PNQC in row_dict else None

    if do_filter is False or (email in args.email or phone_no in args.phone):
      # prioritize using email value over phone number for key
      key = email if email else phone_no
      ppi_data[key] = row_dict

  if len(ppi_data) == 0:
    logging.error("No participants matched filter criteria. aborting.")
    return

  response_json = client.request_json(
      'CheckPpiData',
      method='POST',
      body={'ppi_data': ppi_data})
  _log_ppi_results(response_json['ppi_results'])

def _fetch_csv_data(spreadsheet_id, spreadsheet_gid):
  """
  Download a google doc spreadsheet in CSV format
  :param spreadsheet_id: document id
  :param spreadsheet_gid: gid id
  :return: A list object with rows from spreadsheet
  """
  url = 'https://docs.google.com/spreadsheets/d/%(id)s/export?format=csv&id=%(id)s&gid=%(gid)s' % {
    'id': spreadsheet_id,
    'gid': spreadsheet_gid,
  }
  response = urllib2.urlopen(url)
  if response.code != httplib.OK:  # urllib2 already raises urllib2.HTTPError for some of these.
    raise RuntimeError('Error fetching %r: response %s.' % (url, response.status))

  # Convert csv file to a list of row data
  csv_data = list()
  for row in csv.reader(response):
    csv_data.append(row)

  return csv_data

def _convert_csv_column_to_dict(csv_data, column):
  """
  Return a dictionary object with keys from the first column and values from the specified
  column.
  :param csv_data: File-like CSV text downloaded from Google spreadsheets. (See main doc.)
  :return: dict of fields and values for given column
  """
  results = dict()

  for row in csv_data:
    key = row[0]
    data = row[1:][column]

    if data:
      if key not in results:
        results[key] = data.strip() if data else ''
      else:
        # append multiple choice questions
        results[key] += '|{0}'.format(data.strip())

  return results

def _log_ppi_results(results_json):
  """Formats and logs the validation results. See CheckPpiDataApi for response format details."""
  tests_total = 0
  errors_total = 0
  for email, results in results_json.iteritems():
    tests_count, errors_count = results['tests_count'], results['errors_count']
    errors_total += errors_count
    tests_total += tests_count
    log_lines = [
        'Results for %s: %d tests, %d error%s'
        % (email, tests_count, errors_count, '' if errors_count == 1 else 's')]
    for message in results['error_messages']:
      # Convert braces and unicode indicator to quotes for better readability
      message = re.sub("\[u'", '"', message)
      message = re.sub("'\]", '"', message)
      log_lines += ['\t' + message]
    logging.info('\n'.join(log_lines))
  logging.info(
      'Completed %d tests across %d participants with %d error%s.',
      tests_total, len(results_json), errors_total, '' if errors_total == 1 else 's')

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument(
      'spreadsheet_id',
      help='Google spreadsheet doc ID, after the "/d/" in the URL. The doc must be public.')
  parser.add_argument(
      'spreadsheet_gid',
      help='Google spreadsheet sheet ID, after "gid=" in the URL.')
  parser.add_argument(
      '--email',
      help=('Only validate the given e-mail(s). Validate all by default.'
            ' This flag may be repeated to specify multiple e-mails.'),
      action='append')
  parser.add_argument(
    '--phone',
    help=('Only validate the given phone number. '
          ' This flag may be repeated to specify multiple phone numbers.'),
    action='append')
  rdr_client = Client(parser=parser)
  check_ppi_data(rdr_client, rdr_client.args)
