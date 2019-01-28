import config
import logging
from cloudstorage import cloudstorage_api
from csv import DictReader
from offline.biobank_samples_pipeline import DataError


def mark_ghost_participants():
  # read latest file from csv bucket
  csv_file = get_latest_pid_file()
  csv_reader = DictReader(csv_file)
  # transform participant id's (if needed)

  # write to participant_summary.is_ghost_id column


def get_latest_pid_file():
  bucket = config.getSetting(config.GHOST_ID_BUCKET)
  path = _find_most_recent_file(bucket)
  logging.info('Opening most recent ghost id exclusion list in %r: %r', bucket, path)
  return cloudstorage_api.open(path), path


def _find_most_recent_file(bucket):
  """Return the full path (including bucket name) of the most recently created CSV in the bucket.

  Raises:
    RuntimeError: if no CSVs are found in the cloud storage bucket.
  """
  files_list = cloudstorage_api.listbucket('/' + bucket)
  if not files_list:
    raise DataError('No files in cloud bucket %r.' % bucket)
  # GCS does not really have the concept of directories (it's just a filename convention), so all
  # directory listings are recursive and we must filter out subdirectory contents.
  files_list = [
      s for s in files_list
      if s.filename.lower().endswith('.csv')]
  if not files_list:
    raise DataError(
        'No CSVs in cloud bucket %r (all files: %s).' % (bucket, files_list))
  files_list.sort(key=lambda s: s.st_ctime)
  return files_list[-1].filename
