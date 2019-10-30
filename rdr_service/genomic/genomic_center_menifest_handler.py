"""
Ingest data from a genomic center manifest CSV file which dropped by biobank.
"""

import os
import csv
import datetime
import logging
import collections

from rdr_service import clock
from rdr_service import config
from rdr_service.api_util import list_blobs, open_cloud_file
from rdr_service.config import GENOMIC_GENOTYPING_SAMPLE_MANIFEST_FOLDER_NAME
from rdr_service.dao.genomics_dao import GenomicSetMemberDao
from rdr_service.genomic.genomic_set_file_handler import DataError

_MAX_INPUT_AGE = datetime.timedelta(hours=24)
BIOBANK_ID_PREFIX = 'T'


def process_genotyping_manifest_files():
    bucket_names = config.getSettingList(config.GENOMIC_CENTER_BUCKET_NAME)
    genotyping_folder_name = config.getSetting(GENOMIC_GENOTYPING_SAMPLE_MANIFEST_FOLDER_NAME)

    for bucket_name in bucket_names:
        process_genotyping_manifest_file_from_bucket(bucket_name, genotyping_folder_name)


def process_genotyping_manifest_file_from_bucket(bucket_name, genotyping_folder_name):
    bucket_stat_list = list_blobs(bucket_name)
    if not bucket_stat_list:
        logging.info('No files in cloud bucket %r.' % bucket_name)
        return None
    bucket_stat_list = [s for s in bucket_stat_list if s.name.lower().endswith('.csv')
                        and '%s' % genotyping_folder_name in s.name]
    if not bucket_stat_list:
        logging.info(
            'No CSVs in cloud bucket %r folder %r (all files: %s).' % (bucket_name,
                                                                       genotyping_folder_name,
                                                                       bucket_stat_list))
        return None

    bucket_stat_list.sort(key=lambda s: s.updated)
    path = os.path.normpath(bucket_name + '/' + bucket_stat_list[-1].name)
    timestamp = bucket_stat_list[-1].updated.replace(tzinfo=None)
    logging.info('Opening latest genotyping manifest CSV in %r: %r.', bucket_name + '/'
                 + genotyping_folder_name, path)

    now = clock.CLOCK.now()
    if now - timestamp > _MAX_INPUT_AGE:
        logging.info('Input %r (timestamp %s UTC) is > 24h old (relative to %s UTC), not processing.'
                     % (path, timestamp, now))

        return None
    with open_cloud_file(path) as csv_file:
        update_sample_info_from_genotyping_manifest_file(csv_file)


class CsvColumns(object):
    PACKAGE_ID = 'Package Id'
    SAMPLE_ID = 'Sample Id'
    BIOBANK_ID = 'Biobank Id'
    SAMPLE_TYPE = 'Sample Type'
    TEST_NAME = 'Test Name'

    REQUIRED_COLS = (PACKAGE_ID, SAMPLE_ID, BIOBANK_ID, SAMPLE_TYPE, TEST_NAME)


def update_sample_info_from_genotyping_manifest_file(csv_file):
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    if not set(CsvColumns.REQUIRED_COLS).issubset(set(csv_reader.fieldnames)):
        raise DataError(
            'CSV is missing columns %s, had columns %s.' %
            (CsvColumns.REQUIRED_COLS, csv_reader.fieldnames))

    genotypying_data = collections.namedtuple('genotypingData', [
        'biobank_id',
        'genome_type',
        'sample_id',
        'sample_type',
    ])
    update_queue = collections.deque()

    dao = GenomicSetMemberDao()

    try:
        rows = list(csv_reader)
        for row in rows:
            if row[CsvColumns.BIOBANK_ID] and row[CsvColumns.SAMPLE_ID] and row[CsvColumns.SAMPLE_TYPE] \
                and row[CsvColumns.TEST_NAME]:
                biobank_id = row[CsvColumns.BIOBANK_ID][len(BIOBANK_ID_PREFIX):] \
                    if row[CsvColumns.BIOBANK_ID].startswith(BIOBANK_ID_PREFIX) \
                    else row[CsvColumns.BIOBANK_ID]
                update_queue.append(genotypying_data(
                    biobank_id,
                    row[CsvColumns.TEST_NAME],
                    row[CsvColumns.SAMPLE_ID],
                    row[CsvColumns.SAMPLE_TYPE]
                ))

        dao.bulk_update_genotyping_sample_manifest_data(update_queue)

    except ValueError as e:
        raise DataError(e)
