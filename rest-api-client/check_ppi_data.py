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

import StringIO
import collections
import csv
import httplib
import itertools
import logging
import urllib2

from client import Client
from code_constants import EMAIL_QUESTION_CODE
from main_util import get_parser, configure_logging


def check_ppi_data(client, spreadsheet_id, spreadsheet_gid, emails):
  csv_text = _fetch_csv_data(spreadsheet_id, spreadsheet_gid)
  json_ppi_data = _convert_to_person_dicts(StringIO.StringIO(csv_text), emails)
  response_json = client.request_json(
      'CheckPpiData',
      method='POST',
      body={'ppi_data': json_ppi_data})
  _log_ppi_results(response_json['ppi_results'])


def _fetch_csv_data(spreadsheet_id, spreadsheet_gid):
  url = 'https://docs.google.com/spreadsheets/d/%(id)s/export?format=csv&id=%(id)s&gid=%(gid)s' % {
    'id': spreadsheet_id,
    'gid': spreadsheet_gid,
  }
  response = urllib2.urlopen(url)
  if response.code != httplib.OK:  # urllib2 already raises urllib2.HTTPError for some of these.
    raise RuntimeError('Error fetching %r: response %s.' % (url, response.status))
  return response.read()


def _convert_to_person_dicts(csv_input, raw_include_emails):
  """Converts CSV text to dicts of question/answer values per person.

  Args
    csv_input: File-like CSV text downloaded from Google spreadsheets. (See main doc.)
    raw_include_emails: If non-empty, only include the participants ID'd by these e-mails in the
        returned dict.
  Returns: A nested dictionary of {email: {code: value, code: value, ...}, email: ...}.
      See CheckPpiDataApi for request format details.
  """
  csv_reader = csv.reader(csv_input)
  row_number = 0
  emails = []
  include_emails = []
  emails_to_codes_and_answers = collections.defaultdict(dict)
  for row in csv_reader:
    row_number += 1
    if not row:
      logging.info('Skipping empty CSV row number %d.', row_number)
    answer_code, answer_values = row[0], row[1:]

    # The first row must contain emails.
    if row_number == 1:
      if answer_code != EMAIL_QUESTION_CODE:
        raise ValueError('First row %r does must have values for %r.' % (row, EMAIL_QUESTION_CODE))
      emails = answer_values
      if raw_include_emails:
        logging.info(
            'Only validating %d emails (of %d found in CSV).', len(raw_include_emails), len(emails))
        not_found_emails = set(raw_include_emails) - set(emails)
        if not_found_emails:
          raise ValueError(
              'Spreadsheet had %r, cannot filter to %r (could not find %s).'
              % (emails, raw_include_emails, list(not_found_emails)))
        include_emails = set(raw_include_emails)
      else:
        include_emails = set(emails)
      continue

    # Process remaining rows assuming emails have been read in.
    if len(answer_values) > len(emails):
      raise ValueError(
          'More answers on row %d than emails: %r (emails are %r).'
          % (row_number, answer_values, emails))
    for email, raw_answer_value in itertools.izip_longest(emails, answer_values):
      answer_value = raw_answer_value.strip() if raw_answer_value else ''
      if email in include_emails:
        emails_to_codes_and_answers[email][answer_code] = answer_value
  return dict(emails_to_codes_and_answers)


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
    log_lines += ['\t' + message for message in results['error_messages']]
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
  rdr_client = Client(parser=parser)
  args = rdr_client.args
  check_ppi_data(rdr_client, args.spreadsheet_id, args.spreadsheet_gid, args.email)
