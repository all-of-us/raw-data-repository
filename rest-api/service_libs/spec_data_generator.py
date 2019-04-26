#! /bin/env python
#
# Generate specific fake participant data
#

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import csv
import datetime
import logging
import os
import random
import sys
import time
import traceback
import urllib2

import argparse
import clock
from data_gen.generators import ParticipantGen, BioBankOrderGen, QuestionnaireGen, \
  PhysicalMeasurementsGen, CodeBook
from data_gen.generators.hpo import HPOGen
from services.gcp_utils import gcp_get_app_host_name
from services.gcp_utils import gcp_initialize, gcp_cleanup, gcp_get_app_access_token
from services.system_utils import make_api_request
from services.system_utils import setup_logging, setup_unicode, write_pidfile_or_die, remove_pidfile

_logger = logging.getLogger('rdr_logger')

group = 'spec-gen'
group_desc = 'specific participant data generator'


class DataGeneratorClass(object):

  _gen_url = 'rdr/v1/SpecDataGen'
  _host = None
  _oauth_token = None

  _cb = None
  _p_gen = None
  _pm_gen = None
  _qn_gen = None
  _bio_gen = None

  def __init__(self, args):
    self.args = args

    if args:
      host = gcp_get_app_host_name(self.args.project)
      if self.args.port:
        self._host = '{0}:{1}'.format(host, self.args.port)
      else:
        if host in ['127.0.0.1', 'localhost']:
          self._host = '{0}:{1}'.format(host, 8080)

      if host not in ['127.0.0.1', 'localhost']:
        self._oauth_token = gcp_get_app_access_token()

  def _make_request_header(self):
    """
    make a request headers
    :return: dict
    """
    headers = None
    if self._oauth_token:
      headers = dict()
      headers['Authorization'] = 'Bearer {0}'.format(self._oauth_token)

    return headers

  def _gdoc_csv_data(self, doc_id):
    """
    Fetch a google doc spreadsheet in CSV format
    :param doc_id: document id
    :return: A list object with rows from spreadsheet
    """
    url = 'https://docs.google.com/spreadsheets/d/{0}/export?format=csv'.format(doc_id)
    response = urllib2.urlopen(url)
    if response.code != 200:  # urllib2 already raises urllib2.HTTPError for some of these.
      return None

    # Convert csv file to a list of row data
    csv_data = list()
    for row in csv.reader(response):
      csv_data.append(row)

    return csv_data

  def _local_csv_data(self, filename):
    """
    Read local spreadsheet csv
    :param filename:
    :return:
    """
    if not os.path.exists(filename):
      return None

    csv_data = list()

    # read source spreadsheet into p_data
    with open(filename) as handle:
      reader = csv.reader(handle, delimiter=',')
      for row in reader:
        csv_data.append(row)

    return csv_data

  def _convert_csv_column_to_dict(self, csv_data, column):
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

  def _get_dict_data_by_key(self, data, key):
    """
    Get the data from the dict from the given key
    :param data: dict object
    :param key: string
    :return: data or None
    """
    if not isinstance(data, dict) or not isinstance(key, str):
      raise ValueError('invalid data, unable to return data from dict.')

    if key not in data:
      return None

    return data[key]

  def _random_date(self, start=None, max_delta=None):
    """
    Choose a random date for participant start
    :param start: specific start date.
    :param max_delta: maximum delta from start to use for range.
    :return: datetime
    """
    if not start:
      # set a start date in the past and an end date 40 days in the past
      start = datetime.datetime.now() - datetime.timedelta(weeks=102)
    if max_delta:
      end = start + max_delta
      # don't allow future dates.
      if end > datetime.datetime.now():
        end = self._random_date(start, (datetime.datetime.now() - start))
    else:
      end = datetime.datetime.now() - datetime.timedelta(days=40)
      # if our start is close to now(), just use now().
      if end < start:
        end = datetime.datetime.now()

    # convert to floats and add a random amount of time.
    stime = time.mktime(start.timetuple())
    etime = time.mktime(end.timetuple())
    ptime = stime + (random.random() * (etime - stime))

    # convert to datetime
    ts = time.localtime(ptime)
    dt = datetime.datetime.fromtimestamp(time.mktime(ts))
    # Choose a time somewhere in regular business hours, +0:00 timezone.
    dt = dt.replace(hour=int((4 + random.random() * 12)))
    dt = dt.replace(microsecond=int(random.random() * 999999))

    return dt

  def _increment_date(self, dt, minute_range=None, day_range=None):
    """
    Increment the timestamp a bit.
    :param dt: datetime value to use for incrementing.
    :param minute_range: range to choose random minute value from.
    :param day_range: range to choose random day value from.
    :return: datetime
    """
    if minute_range:
      dt += datetime.timedelta(minutes=int(random.random() * minute_range))
    else:
      dt += datetime.timedelta(minutes=int(random.random() * 20))

    if day_range:
      dt += datetime.timedelta(days=int(random.random() * day_range))
      # Choose a time somewhere in regular business hours, +0:00 timezone.
      dt = dt.replace(hour=int((4 + random.random() * 12)))
      dt = dt.replace(microsecond=int(random.random() * 999999))

    return dt

  def create_participant(self, site_id=None, hpo_id=None):
    """
    Create a new participant with a random or specific hpo or site id
    :param site_id: name of specific hpo site
    :param hpo_id: name of hpo
    :return: participant object
    """
    hpo_site = None
    hpo_gen = HPOGen()

    if site_id:
      # if site_id is given, it also returns the HPO the site is matched with.
      hpo_site = hpo_gen.get_site(site_id)
    if hpo_id and not hpo_site:
      # if hpo is given, select a random site within the hpo.
      hpo_site = hpo_gen.get_hpo(hpo_id).get_random_site()
    if not hpo_site:
      # choose a random hpo and site.
      hpo_site = hpo_gen.get_random_site()
    # initialize participant generator.
    if not self._p_gen:
      self._p_gen = ParticipantGen()

    # make a new participant.
    p_obj = self._p_gen.new(hpo_site)

    data = dict()
    data['api'] = 'Participant'
    data['data'] = p_obj.to_dict()
    data['timestamp'] = clock.CLOCK.now().isoformat()

    code, resp = make_api_request(self._host, self._gen_url, req_type='post', json_data=data,
                                  headers=self._make_request_header())
    if code == 200 and resp:
      p_obj.update(resp)
      return p_obj, hpo_site

    raise ValueError('invalid response, failed to create participant [Http {0}: {1}].'.format(code, resp))

  def submit_physical_measurements(self, participant_id, site):
    """
    Create a physical measurements response for the participant
    :param participant_id: participant id
    :param site: HPOSiteGen object
    :return: True if POST request is successful otherwise False.
    """
    if not self._pm_gen:
      self._pm_gen = PhysicalMeasurementsGen()

    pm_obj = self._pm_gen.new(participant_id, site)

    data = dict()
    data['api'] = 'Participant/{0}/PhysicalMeasurements'.format(participant_id)
    data['data'] = pm_obj.make_fhir_document()
    # make the submit time a little later than the authored timestamp.
    data['timestamp'] = clock.CLOCK.now().isoformat()

    code, resp = make_api_request(self._host, self._gen_url, req_type='post', json_data=data,
                                  headers=self._make_request_header())

    if code == 200:
      pm_obj.update(resp)
      return pm_obj

    raise ValueError('invalid response, failed to create module response [Http {0}: {1}].'.format(code, resp))

  def submit_biobank_order(self, participant_id, sample_test, site, to_mayo=False):
    """
    Create a biobank order response for the participant
    :param participant_id: participant id
    :param sample_test: sample test code
    :param site: HPOSiteGen object
    :param to_mayo: if True, also send order to Mayolink.
    :return: True if POST request is successful otherwise False.
    """
    if not sample_test:
      return None

    if not self._bio_gen:
      self._bio_gen = BioBankOrderGen()

    bio_obj = self._bio_gen.new(participant_id, sample_test, site)

    data = dict()
    data['api'] = 'Participant/{0}/BiobankOrder'.format(participant_id)
    data['data'], finalized = bio_obj.make_fhir_document()
    # make the submit time a little later than the finalized timestamp.
    data['timestamp'] = self._increment_date(finalized, minute_range=15).isoformat()
    data['mayolink'] = to_mayo

    code, resp = make_api_request(self._host, self._gen_url, req_type='post', json_data=data,
                                  headers=self._make_request_header())
    if code == 200:
      bio_obj.update(resp)
      return bio_obj

    raise ValueError('invalid response, failed to create module response [Http {0}: {1}].'.format(code, resp))

  def submit_module_response(self, module_id, participant_id, overrides=None):
    """
    Create a questionnaire response for the given module.
    :param module_id: questionnaire module name
    :param participant_id: participant id
    :param overrides: list of tuples giving answers to specific questions.
    :return: True if POST request is successful otherwise False.
    """
    if not module_id or not isinstance(module_id, str):
      raise ValueError('invalid module id.')
    if not participant_id or not isinstance(str(participant_id), str):
      raise ValueError('invalid participant id.')

    if not self._cb:
      # We only want to create these once, because they download data from github.
      self._cb = CodeBook()
      self._qn_gen = QuestionnaireGen(self._cb, self._host)

    qn_obj = self._qn_gen.new(module_id, participant_id, overrides)

    data = dict()
    data['api'] = 'Participant/{0}/QuestionnaireResponse'.format(participant_id)
    data['data'] = qn_obj.make_fhir_document()
    # make the submit time a little later than the authored timestamp.
    data['timestamp'] = clock.CLOCK.now().isoformat()

    code, resp = make_api_request(self._host, self._gen_url, req_type='post', json_data=data,
                                  headers=self._make_request_header())
    if code == 200:
      qn_obj.update(resp)
      return qn_obj

    raise ValueError('invalid response, failed to create module response [Http {0}: {1}].'.format(code, resp))

  def run(self):
    """
    Main program process
    :param args: program arguments
    :return: Exit code value
    """
    # load participant spreadsheet from bucket or local file.
    csv_data = self._local_csv_data(self.args.src_csv) or self._gdoc_csv_data(self.args.src_csv)
    if not csv_data:
      _logger.error('unable to fetch participant source spreadsheet [{0}].'.format(self.args.src_csv))
      return 1

    _logger.info('processing source data.')
    count = 0

    # Loop through each column and generate data.
    for column in range(0, len(csv_data[0]) - 1):

      p_data = self._convert_csv_column_to_dict(csv_data, column)

      hpo = self._get_dict_data_by_key(p_data, '_HPO')
      pm = self._get_dict_data_by_key(p_data, '_PM')
      site_id = self._get_dict_data_by_key(p_data, '_HPOSite')
      bio_orders = self._get_dict_data_by_key(p_data, '_BIOOrder')
      bio_orders_mayo = self._get_dict_data_by_key(p_data, '_BIOOrderMayo')
      ppi_modules = self._get_dict_data_by_key(p_data, '_PPIModule')

      # choose a random starting date, timestamps of all other activities feed off this value.
      start_dt = self._random_date()
      #
      # Create a new participant
      #
      count += 1
      _logger.info('Participant [{0}]'.format(count))
      with clock.FakeClock(start_dt):
        p_obj, hpo_site = self.create_participant(site_id=site_id, hpo_id=hpo)

        if not p_obj or 'participantId' not in p_obj.__dict__:
          _logger.error('failed to create participant')
          continue

        _logger.info('  Created [{0}]'.format(p_obj.participantId))
      #
      # process any questionnaire modules
      #
      if ppi_modules:

        # submit the first module pretty close to the start date. Assumes the first
        # module is ConsentPII.
        mod_dt = self._increment_date(start_dt, minute_range=60)

        modules = ppi_modules.split('|')
        for module in modules:
          with clock.FakeClock(mod_dt):
            mod_obj = self.submit_module_response(module, p_obj.participantId, p_data.items())
            if mod_obj:
              _logger.info('  Module: [{0}]: submitted.'.format(module))
            else:
              _logger.info('  Module: [{0}]: failed.'.format(module))
          #
          # see if we need to submit physical measurements.
          #
          if module == 'ConsentPII' and pm and pm.lower() == 'yes':

            mod_dt = self._random_date(mod_dt, datetime.timedelta(minutes=90))
            with clock.FakeClock(mod_dt):
              pm_obj = self.submit_physical_measurements(p_obj.participantId, hpo_site)
              if pm_obj:
                _logger.info('  PM: submitted.')
              else:
                _logger.info('  PM: failed.')
          # choose a new random date between mod_dt and mod_dt + 15 days.
          mod_dt = self._random_date(mod_dt, datetime.timedelta(days=15))
      #
      # process biobank samples
      #
      if bio_orders:
        sample_dt = self._increment_date(start_dt, day_range=10)

        samples = bio_orders.split('|')
        for sample in samples:
          with clock.FakeClock(sample_dt):
            bio_obj = self.submit_biobank_order(p_obj.participantId, sample, hpo_site)
            if bio_obj:
              _logger.info('  BioBank Order: [{0}] submitted.'.format(sample))
            else:
              _logger.info('  BioBank Order: [{0}] failed.'.format(sample))

          sample_dt = self._random_date(sample_dt, datetime.timedelta(days=30))

      #
      # process biobank samples that also need to be sent to Mayolink.
      #
      if bio_orders_mayo:
        sample_dt = self._increment_date(start_dt, day_range=10)

        samples = bio_orders_mayo.split('|')
        for sample in samples:
          with clock.FakeClock(sample_dt):
            bio_obj = self.submit_biobank_order(p_obj.participantId, sample, hpo_site, to_mayo=True)
            if bio_obj:
              _logger.info('  BioBank Order w/Mayo: {0} submitted.'.format(sample))
            else:
              _logger.info('  BioBank Order w/Mayo: {0} failed.'.format(sample))

          sample_dt = self._random_date(sample_dt, datetime.timedelta(days=30))

    return 0


def run():
  # Set global debug value and setup application logging.
  setup_logging(_logger, group, '--debug' in sys.argv, '{0}.log'.format(group) if '--log-file' in sys.argv else None)
  setup_unicode()
  exit_code = 1

  # Setup program arguments.
  parser = argparse.ArgumentParser(prog=group, description=group_desc)
  parser.add_argument('--debug', help='Enable debug output', default=False, action='store_true')  # noqa
  parser.add_argument('--log-file', help='write output to a log file', default=False, action='store_true')  # noqa
  parser.add_argument('--project', help='gcp project name', default='localhost')  # noqa
  parser.add_argument('--account', help='pmi-ops account', default=None)  # noqa
  parser.add_argument('--service-account', help='gcp iam service account', default=None)  # noqa
  parser.add_argument('--port', help='alternate ip port to connect to', default=None)  # noqa
  parser.add_argument('--src-csv', help='participant list csv (file/google doc id)', required=True)  # noqa
  args = parser.parse_args()

  # Ensure only one copy of the program is running at the same time
  write_pidfile_or_die(group)
  # initialize gcp environment.
  env = gcp_initialize(args.project, args.account, args.service_account)
  if not env:
    remove_pidfile(group)
    exit(exit_code)
  # verify we're not getting pointed to production.
  if env['project'] == 'all-of-us-rdr-prod':
    _logger.error('using spec generator in production is not allowed.')
    remove_pidfile(group)
    exit(exit_code)

  try:
    process = DataGeneratorClass(args)
    exit_code = process.run()
  except IOError:
    _logger.error('io error')
  except Exception:
    print(traceback.format_exc())
    _logger.error('program encountered an unexpected error, quitting.')
  finally:
    gcp_cleanup(args.account)
    remove_pidfile(group)

  _logger.info('done')
  exit(exit_code)


# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
