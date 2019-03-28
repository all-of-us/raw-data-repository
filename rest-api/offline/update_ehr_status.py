import calendar
import collections
import csv
import datetime
import json
import re

import cloudstorage
from cloudstorage import cloudstorage_api

import clock
import config
from cloud_utils.google_sheets import GoogleSheetCSVReader
from dao.ehr_dao import EhrReceiptDao
from dao.organization_dao import OrganizationDao
from dao.participant_summary_dao import ParticipantSummaryDao
from google.appengine.ext import deferred
from model.ehr import EhrReceipt


HPO_REPORT_CONFIG_GCS_PATH = '/all-of-us-rdr-sequestered-config-test/hpo-report-config-mixin.json'

COLUMN_BUCKET_NAME = 'Bucket Name'
COLUMN_ORG_EXTERNAL_ID = 'Org ID'


SubmissionInfo = collections.namedtuple('SubmissionInfo', ('bucket_name', 'date', 'person_file'))
OrganizationInfo = collections.namedtuple('OrganizationInfo',
                                          ('id', 'submission_date', 'person_file'))


def update_ehr_status():
  """
  Entrypoint, executed as a cron job
  """
  now = clock.CLOCK.now()
  cutoff_date = (now - datetime.timedelta(days=1)).date()
  bucket_name = _get_curation_bucket_name()
  organization_info_list = _get_organization_info_list(
    cloudstorage_api.listbucket('/' + bucket_name),
    cutoff_date
  )
  for org_info in organization_info_list:
    deferred.defer(_do_update_for_organization, *org_info)


def _do_update_for_organization(organization_id, submission_date, person_file):
  """
  deferred task: creates EhrReceipt and updates ParticipantSummary objects from a person.csv file
  """
  updated_datetime = datetime.datetime.combine(submission_date, datetime.datetime.min.time())

  org_dao = OrganizationDao()
  summary_dao = ParticipantSummaryDao()
  receipt_dao = EhrReceiptDao()

  org = org_dao.get_by_external_id(organization_id.upper())

  receipt = EhrReceipt(organizationId=org.organizationId, receiptTime=updated_datetime)
  receipt_dao.insert(receipt)

  for participant_id in _get_participant_ids_from_person_file(person_file):
    summary = summary_dao.get(participant_id)
    summary_dao.update_ehr_status(summary, updated_datetime)
    summary_dao.update(summary)


def _get_curation_bucket_name():
  """
  Get the name of the bucket to read from
  """
  return config.getSetting(config.CURATION_BUCKET_NAME)


def _get_sheet_id():
  """
  Get the google sheet id for the bucket-name-to-organization-external-id mapping
  """
  with cloudstorage.open(HPO_REPORT_CONFIG_GCS_PATH, 'r') as handle:
    hpo_config = json.load(handle)
  sheet_id = hpo_config.get('hpo_report_google_sheet_id')
  if sheet_id is None:
    raise ValueError("Missing config value: hpo_report_google_sheet_id")
  return sheet_id


def _get_org_id_by_bucket_name_map():
  """
  Create a dictionary mapping of bucket-name-to-organization-external-id
  """
  return {
    row.get(COLUMN_BUCKET_NAME): row.get(COLUMN_ORG_EXTERNAL_ID)
    for row in GoogleSheetCSVReader(_get_sheet_id())
  }


def _get_organization_info_list(bucket_stat_list, cutoff_date):
  """
  Create a list of OrganizationInfo objects representing the latest person.csv submission
  for each organization in the provided bucket listing.
  Only include submissions after the cutoff_date.
  """
  def _iter_submission_infos():
    cutoff_ctime = calendar.timegm(cutoff_date.timetuple())
    for stat in bucket_stat_list:
      if stat.st_ctime < cutoff_ctime:
        continue
      if not stat.filename.endswith('person.csv'):
        continue
      info_obj = _get_submission_info_from_filename(stat.filename)
      if info_obj and info_obj.date and info_obj.date > cutoff_date:
        yield info_obj

  def keep_latest_reducer(accumulator, next_info_obj):
    existing_info = accumulator.get(next_info_obj.bucket_name)
    if not existing_info or existing_info.date < next_info_obj.date:
      accumulator[next_info_obj.bucket_name] = next_info_obj
    return accumulator

  org_id_by_bucket_name_map = _get_org_id_by_bucket_name_map()
  submission_info_by_bucket_map = reduce(keep_latest_reducer, _iter_submission_infos(), {})
  organization_info_list = [
    OrganizationInfo(
      org_id_by_bucket_name_map.get(submission_info.bucket_name),
      submission_info.date,
      submission_info.person_file
    )
    for submission_info
    in submission_info_by_bucket_map.values()
  ]
  return filter(lambda o: o.id is not None, organization_info_list)


def _get_submission_info_from_filename(person_filename):
  """
  create a SubmissionInfo object from a person.csv file's full GCS filename
  """
  try:
    _, _, upload_bucket_name, submission_name, _ = person_filename.lstrip('/').split('/')
    submission_date = _parse_date_from_submission_name(submission_name)
  except ValueError:
    return None
  return SubmissionInfo(upload_bucket_name, submission_date, person_filename)


def _parse_date_from_submission_name(submission):
  """
  The modified time of the item cannot be trusted so we must rely on the filename date
  a submission directory is named manually by the uploaders so it does not have one universal format
  NOTE: any unparsable dates will be ignored
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
