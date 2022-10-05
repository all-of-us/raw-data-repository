import os
import datetime
import json

from rdr_service import clock, config
from rdr_service.api_util import list_blobs, delete_cloud_file
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.api_util import open_cloud_file

_INPUT_FILENAME_TIME_FORMAT_LENGTH = 18
_JSON_SUFFIX_LENGTH = 5
INPUT_FILENAME_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
_MAX_FILE_AGE = datetime.timedelta(days=7)
FILE_PREFIX = 'va_daily_participant_wq_'


def generate_workqueue_report():
    """ Creates csv file from ParticipantSummary table for participants paired to VA """
    hpo_dao = HPODao()
    summary_dao = ParticipantSummaryDao()
    bucket = config.getSetting(config.VA_WORKQUEUE_BUCKET_NAME)
    subfolder = config.getSetting(config.VA_WORKQUEUE_SUBFOLDER)
    file_timestamp = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")
    file_name = f'{FILE_PREFIX}{file_timestamp}.json'
    participants = summary_dao.get_by_hpo(hpo_dao.get_by_name('VA'))
    participants_to_export = []
    for participant in participants:
        participants_to_export.append(summary_dao.to_client_json(participant))
    export_data = json.dumps(participants_to_export)
    export_path = f'/{bucket}/{subfolder}/{file_name}'
    with open_cloud_file(export_path, mode='w') as export_file:
        export_file.write(export_data)


def delete_old_reports():
    """ Deletes export files that are more than 7 days old """
    bucket = config.getSetting(config.VA_WORKQUEUE_BUCKET_NAME)
    subfolder = config.getSetting(config.VA_WORKQUEUE_SUBFOLDER)
    now = clock.CLOCK.now()
    for file in list_blobs(bucket, subfolder):
        if file.name.endswith(".json") and os.path.basename(file.name).startswith(FILE_PREFIX):
            file_time = _timestamp_from_filename(file.name)
            if now - file_time > _MAX_FILE_AGE:
                delete_cloud_file(bucket + "/" + file.name)


def _timestamp_from_filename(filename):
    if len(filename) < _INPUT_FILENAME_TIME_FORMAT_LENGTH + _JSON_SUFFIX_LENGTH:
        raise RuntimeError(f"Can't parse time from filename: {filename}")
    time_suffix = filename[len(filename) - (_INPUT_FILENAME_TIME_FORMAT_LENGTH + _JSON_SUFFIX_LENGTH) - 1:
                           len(filename) - _JSON_SUFFIX_LENGTH]
    try:
        timestamp = datetime.datetime.strptime(time_suffix, INPUT_FILENAME_TIME_FORMAT)
    except ValueError as timestamp_parse_error:
        raise RuntimeError(f"Can't parse time from filename: {filename}") from timestamp_parse_error
    return timestamp
