import logging
import os

from csv import DictReader
from rdr_service import config
from rdr_service.api_util import open_cloud_file, list_blobs
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.offline.biobank_samples_pipeline import DataError


def mark_ghost_participants():
    bucket = config.getSetting(config.GHOST_ID_BUCKET)
    # read latest file from csv bucket
    p_dao = ParticipantDao()
    csv_file_path, file_name = get_latest_pid_file(bucket)
    logging.info("Getting list of ghost accounts from %s", file_name)
    with open_cloud_file(csv_file_path) as csv_file_obj:
        csv_reader = DictReader(csv_file_obj)

        for row in csv_reader:
            pid = row.get("participant_id")

            with p_dao.session() as session:
                p_dao.update_ghost_participant(session, pid)
                logging.info("Added ghost flag to %s ", pid)


def get_latest_pid_file(bucket):
    blob_name = _find_most_recent_file(bucket)
    file_name = os.path.basename(blob_name)
    path = os.path.normpath(bucket + '/' + blob_name)
    logging.info("Opening most recent ghost id exclusion list in %r: %r", blob_name, bucket)
    return path, file_name


def _find_most_recent_file(bucket):
    """Return the full path (including bucket name) of the most recently created CSV in the bucket.

  Raises:
    RuntimeError: if no CSVs are found in the cloud storage bucket.
  """
    files_list = list_blobs(bucket)
    if not files_list:
        raise DataError("No files in cloud bucket %r." % bucket)
    files_list = [s for s in files_list if s.name.lower().endswith(".csv")]
    if not files_list:
        raise DataError("No CSVs in cloud bucket %r (all files: %s)." % (bucket, files_list))
    files_list.sort(key=lambda s: s.updated)
    return files_list[-1].name
