import logging
from csv import DictReader

import config
from cloudstorage import cloudstorage_api
from dao.participant_dao import ParticipantDao
from offline.biobank_samples_pipeline import DataError


def mark_ghost_participants():
  bucket = config.getSetting(config.GHOST_ID_BUCKET)
  # read latest file from csv bucket
  p_dao = ParticipantDao()
  csv_file_obj, file_name = get_latest_pid_file(bucket)
  logging.info('Getting list of ghost accounts from %s', file_name)
  csv_reader = DictReader(csv_file_obj)

  for row in csv_reader:
    pid = row.get('participant_id')

    with p_dao.session() as session:
      p_dao.update_ghost_participant(session, pid)
      logging.info('Added ghost flag to %s ', pid)


def get_latest_pid_file(bucket):
  path = _find_most_recent_file(bucket)
  logging.info('Opening most recent ghost id exclusion list in %r: %r', path, bucket)
  return cloudstorage_api.open(path), path


def _find_most_recent_file(bucket):
  """Return the full path (including bucket name) of the most recently created CSV in the bucket.

  Raises:
    RuntimeError: if no CSVs are found in the cloud storage bucket.
  """
  files_list = cloudstorage_api.listbucket('/' + bucket)
  if not files_list:
    raise DataError('No files in cloud bucket %r.' % bucket)
  files_list = [
      s for s in files_list
      if s.filename.lower().endswith('.csv')]
  if not files_list:
    raise DataError(
        'No CSVs in cloud bucket %r (all files: %s).' % (bucket, files_list))
  files_list.sort(key=lambda s: s.st_ctime)
  return files_list[-1].filename
