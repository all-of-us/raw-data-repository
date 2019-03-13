import calendar
import csv
import datetime
import re

import clock
import cloud_utils.bigquery
import config
from cloudstorage import cloudstorage_api
from dao.ehr_dao import EhrReceiptDao
from dao.hpo_dao import HPODao
from dao.participant_summary_dao import ParticipantSummaryDao
from google.appengine.ext import deferred
from model.ehr import EhrReceipt
from participant_enums import EhrStatus


def update_ehr_status():
  bucket_name = _get_curation_bucket_name()
  now = clock.CLOCK.now()
  cutoff_date = (now - datetime.timedelta(days=1)).date()
  for hpo_info_dict in _get_hpos_updated_from_file_stat_list_since_datetime(
    cloudstorage_api.listbucket('/' + bucket_name),
    cutoff_date
  ):
    deferred.defer(
      _do_update_hpo,
      **hpo_info_dict
    )


def _get_curation_bucket_name():
  return config.getSetting(config.CURATION_BUCKET_NAME)


def _query_ehr_upload_pids():
  query_string = 'select * from `{project}.{dataset}.{view}` limit 1'.format(
    project=config.getSetting(config.CURATION_BIGQUERY_PROJECT),
    dataset='operations_analytics',
    view='ehr_upload_pids'
  )
  return cloud_utils.bigquery.bigquery(query_string)


def _query_hpo_counts():
  query_string = 'select * from `{project}.{dataset}.{view}` limit 1'.format(
    project=config.getSetting(config.CURATION_BIGQUERY_PROJECT),
    dataset='operations_analytics',
    view='table_counts_with_upload_timestamp_for_hpo_sites'
  )
  return cloud_utils.bigquery.bigquery(query_string)


def _get_hpos_updated_from_file_stat_list_since_datetime(bucket_stat_list, cutoff_date):
  """
  gets a list of most recent `person.csv` files uploaded by HPOs.
  designed to work on curation internal bucket

  :param bucket_stat_list: output from `cloudstorage_api.listbucket()`
  :param cutoff_date: earliest date that should be included in the results
  :return: list of hpo_info_dicts {'hpo_id_string', 'filename', 'updated_date'}
  """
  cutoff_ctime = calendar.timegm(cutoff_date.timetuple())

  def reduce_newest_info_to_dict(accumulator, x):
    """
    reduces a list of hpo_info_dicts to a dict of [hpo_id_string]=hpo_info_dict
    keeps only the newest hpo_info_dict for each hpo_id_string
    """
    key = x['hpo_id_string']
    existing = accumulator.get(key)
    if existing:
      if existing['updated_date'] > x['updated_date']:
        return accumulator
      if (
        existing['updated_date'] == x['updated_date']
        and existing['person_file'] > x['person_file']
      ):
        return accumulator
    return dict(accumulator, **{key: x})

  newest_hpo_info_dict = reduce(
    reduce_newest_info_to_dict,
    [
      {
        'hpo_id_string': hpo_id_string,
        'person_file': file_stat.filename,
        'updated_date': updated_date,
      }
      for file_stat, hpo_id_string, updated_date
      in [
        (stat,) + _parse_hpo_id_and_date_from_person_filename(stat.filename)
        for stat
        in bucket_stat_list
        if stat.filename.lower().endswith('person.csv')
        and stat.st_ctime >= cutoff_ctime  # initial filtering from creation time
      ]
      if updated_date and updated_date >= cutoff_date  # real filtering based on date in name
    ],
    {}  # initial
  )

  return newest_hpo_info_dict.values()


def _parse_hpo_id_and_date_from_person_filename(person_file_path):
  try:
    _, hpo_id_string, _, submission_name, _ = person_file_path.lstrip('/').split('/')
    submission_date = _parse_date_from_submission_name(submission_name)
  except ValueError:
    return None, None
  return hpo_id_string, submission_date


def _parse_date_from_submission_name(submission):
  """The modified time of the item cannot be trusted so we must rely on the filename date
  a submission directory is named manually by the uploaders so it does not have one universal format
  """
  date_pattern = re.compile(r'(\d{4}[-_]?\d{1,2}[-_]?\d{1,2})')
  date_format_options = [
    '%Y-%m-%d',
    '%Y%m%d'
  ]
  match = re.search(date_pattern, submission)
  if not match:
    return None
  for format_string in date_format_options:
    try:
      return datetime.datetime.strptime(match.group(), format_string).date()
    except ValueError:
      pass


def _get_participant_ids_from_person_file(person_file):
  """
  reads the specified CSV file from cloud storage and returns a list of the first column integers
  """
  def parse_pid(row):
    try:
      return int(row[0])
    except (KeyError, ValueError):
      pass

  with cloudstorage_api.open(person_file) as gcs_file:
    return filter(bool, map(parse_pid, csv.reader(gcs_file)))


def _do_update_hpo(hpo_id_string=None, person_file=None, updated_date=None):
  updated_datetime = datetime.datetime.combine(updated_date, datetime.datetime.min.time())

  hpo_dao = HPODao()
  summary_dao = ParticipantSummaryDao()
  receipt_dao = EhrReceiptDao()

  hpo = hpo_dao.get_by_name(hpo_id_string)  # TODO: confirm this lookup is valid

  receipt = EhrReceipt(hpoId=hpo.hpoId, receiptTime=updated_datetime)
  receipt_dao.insert(receipt)

  for participant_id in _get_participant_ids_from_person_file(person_file):
    summary = summary_dao.get(participant_id)
    summary.ehrStatus = EhrStatus.PRESENT
    if not summary.ehrReceiptTime:
      summary.ehrReceiptTime = updated_datetime
    summary.ehrUpdateTime = updated_datetime
