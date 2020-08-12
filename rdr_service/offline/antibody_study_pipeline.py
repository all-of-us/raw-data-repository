import logging
import datetime
import os
import csv
import pytz

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file, list_blobs, copy_cloud_file
from rdr_service.model.covid_antibody_study import BiobankCovidAntibodySample, QuestCovidAntibodyTest, \
    QuestCovidAntibodyTestResult
from rdr_service.dao.antibody_study_dao import BiobankCovidAntibodySampleDao, QuestCovidAntibodyTestDao, \
    QuestCovidAntibodyTestResultDao
from rdr_service.model.config_utils import get_biobank_id_prefix

_MANIFEST_FILE_SUB_FOLDER_NAME = 'antibody_manifests'
_MANIFEST_FILE_NAME_PREFIX = 'Quest_AoU_Serology'
_QUEST_AOU_RESULTS_FILE_NAME_PREFIX = 'Quest_AoU_Results'
_QUEST_AOU_TESTS_FILE_NAME_PREFIX = 'Quest_AoU_Tests'
_CLIA_COMPLIANCE_SUB_FOLDER_NAME = 'clia-compliant-reports'
_BATCH_SIZE = 100
BIOBANK_ID_PREFIX = get_biobank_id_prefix()


def import_biobank_covid_manifest_files():
    bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)
    _import_file(bucket_name, _MANIFEST_FILE_SUB_FOLDER_NAME, _MANIFEST_FILE_NAME_PREFIX,
                 _save_biobank_antibody_manifest_from_csv)


def import_quest_antibody_files():
    bucket_name = config.getSetting(config.QUEST_ANTIBODY_STUDY_BUCKET_NAME)
    _import_file(bucket_name, None, _QUEST_AOU_TESTS_FILE_NAME_PREFIX, _save_quest_covid_antibody_test_from_csv)
    _import_file(bucket_name, None, _QUEST_AOU_RESULTS_FILE_NAME_PREFIX,
                 _save_quest_covid_antibody_test_result_from_csv)


def sync_clia_compliance_pdf_files():
    antibody_study_bucket_name = config.getSetting(config.QUEST_ANTIBODY_STUDY_BUCKET_NAME)
    ptc_target_bucket = config.getSetting(config.PTC_CLIA_COMPLIANT_REPORT_BUCKET_NAME)
    ce_target_bucket = config.getSetting(config.CE_CLIA_COMPLIANT_REPORT_BUCKET_NAME)

    file_list = _find_file_list(antibody_study_bucket_name, sub_folder_name=_CLIA_COMPLIANCE_SUB_FOLDER_NAME,
                                file_suffix_name='.pdf')
    biobank_covid_antibody_dao = BiobankCovidAntibodySampleDao()
    with biobank_covid_antibody_dao.session() as session:
        for (file_cloud_path, filename) in file_list:
            try:
                specimen_id = filename[filename.rindex('-')+1: filename.rindex('.')]
            except ValueError:
                logging.info("Invalid CLIA compliance pdf file name: {}.".format(filename))
                continue
            biobank_id = biobank_covid_antibody_dao.get_biobank_id_by_sample_id_with_session(session, specimen_id)
            if biobank_id is None:
                logging.info("Skip file {}, no biobank ID found for specimen ID {}".format(filename, specimen_id))
                continue

            target_file_name = '{}.pdf'.format(biobank_id)

            # cp to ptc bucket
            target_path = ptc_target_bucket + '/' + target_file_name
            logging.info('copy {}, from {} to {}'.format(filename, file_cloud_path, target_path))
            copy_cloud_file(file_cloud_path, target_path)
            # cp to ce bucket
            target_path = ce_target_bucket + '/' + target_file_name
            logging.info('copy {}, from {} to {}'.format(filename, file_cloud_path, target_path))
            copy_cloud_file(file_cloud_path, target_path)


def _import_file(bucket_name, sub_folder_name, file_name_prefix, func):
    logging.info("Starting antibody study data import from {}.".format(bucket_name))
    try:
        file_list = _find_file_list(bucket_name, file_name_prefix, sub_folder_name, file_suffix_name='.csv')
    except FileNotFoundError:
        logging.info("File not found")
        return None
    if not file_list:
        logging.info("No file found for importing")
        return None
    for (csv_file_cloud_path, csv_filename) in file_list:
        with open_cloud_file(csv_file_cloud_path) as csv_file:
            logging.info("Opening  CSV in {}: {}.".format(bucket_name, csv_file_cloud_path))
            csv_reader = csv.DictReader(csv_file, delimiter=",")
            func(csv_reader, csv_filename)
            logging.info("import completed, file name: {}".format(csv_filename))
    logging.info("import all files completed")


def _save_biobank_antibody_manifest_from_csv(csv_reader, csv_filename):
    missing_cols = set(BiobankAntibodyManifestCsvColumns.ALL) - set(csv_reader.fieldnames)
    if missing_cols:
        raise DataError("CSV is missing columns %s, had columns %s." % (missing_cols, csv_reader.fieldnames))

    biobank_covid_antibody_dao = BiobankCovidAntibodySampleDao()
    records = []
    rows = list(csv_reader)
    with biobank_covid_antibody_dao.session() as session:
        for row in rows:
            if not row[BiobankAntibodyManifestCsvColumns.SAMPLE_ID]:
                continue
            record = _create_biobank_covid_antibody_obj_from_row(row, csv_filename)
            records.append(record)
            if len(records) >= _BATCH_SIZE:
                biobank_covid_antibody_dao.upsert_all_with_session(session, records)
                records = []
        if records:
            biobank_covid_antibody_dao.upsert_all_with_session(session, records)


def _create_biobank_covid_antibody_obj_from_row(row, csv_filename):
    biobank_id = row[BiobankAntibodyManifestCsvColumns.BIOBANK_ID]
    aou_biobank_id = no_aou_biobank_id = None

    if biobank_id and biobank_id.startswith(BIOBANK_ID_PREFIX) and len(biobank_id) == 10:
        aou_biobank_id = biobank_id[len(BIOBANK_ID_PREFIX):]
    else:
        no_aou_biobank_id = biobank_id

    kwargs = dict(
        aouBiobankId=aou_biobank_id,
        noAouBiobankId=no_aou_biobank_id,
        sampleId=row[BiobankAntibodyManifestCsvColumns.SAMPLE_ID],
        matrixTubeId=row[BiobankAntibodyManifestCsvColumns.MATRIX_ID],
        sampleType=row[BiobankAntibodyManifestCsvColumns.SAMPLE_TYPE],
        quantityUl=row[BiobankAntibodyManifestCsvColumns.QUANTITY],
        storageLocation=row[BiobankAntibodyManifestCsvColumns.STORAGE_LOCATION],
        collectionDate=row[BiobankAntibodyManifestCsvColumns.COLLECTION_DATE]
        if row[BiobankAntibodyManifestCsvColumns.COLLECTION_DATE] else None,
        ingestFileName=csv_filename
    )

    obj = BiobankCovidAntibodySample(**kwargs)

    return obj


def _save_quest_covid_antibody_test_from_csv(csv_reader, csv_filename):
    missing_cols = set(QuestCovidAntibodyTestCsvColumns.ALL) - set(csv_reader.fieldnames)
    if missing_cols:
        raise DataError("CSV is missing columns %s, had columns %s." % (missing_cols, csv_reader.fieldnames))

    quest_covid_antibody_test_dao = QuestCovidAntibodyTestDao()
    records = []
    rows = list(csv_reader)
    with quest_covid_antibody_test_dao.session() as session:
        for row in rows:
            if not row[QuestCovidAntibodyTestCsvColumns.ACCESSION]:
                continue
            record = _create_quest_covid_antibody_test_obj_from_row(row, csv_filename)
            records.append(record)
            if len(records) >= _BATCH_SIZE:
                quest_covid_antibody_test_dao.upsert_all_with_session(session, records)
                records = []
        if records:
            quest_covid_antibody_test_dao.upsert_all_with_session(session, records)


def _create_quest_covid_antibody_test_obj_from_row(row, csv_filename):
    kwargs = dict(
        specimenId=row[QuestCovidAntibodyTestCsvColumns.SPECIMEN_ID],
        testCode=row[QuestCovidAntibodyTestCsvColumns.TEST_CODE],
        testName=row[QuestCovidAntibodyTestCsvColumns.TEST_NAME],
        runDateTime=row[QuestCovidAntibodyTestCsvColumns.RUN_DATE_TIME]
        if row[QuestCovidAntibodyTestCsvColumns.RUN_DATE_TIME] else None,
        accession=row[QuestCovidAntibodyTestCsvColumns.ACCESSION],
        instrumentName=row[QuestCovidAntibodyTestCsvColumns.INSTRUMENT_NAME],
        position=row[QuestCovidAntibodyTestCsvColumns.POSITION],
        ingestFileName=csv_filename
    )

    obj = QuestCovidAntibodyTest(**kwargs)

    return obj


def _save_quest_covid_antibody_test_result_from_csv(csv_reader, csv_filename):
    missing_cols = set(QuestCovidAntibodyTestResultCsvColumns.ALL) - set(csv_reader.fieldnames)
    if missing_cols:
        raise DataError("CSV is missing columns %s, had columns %s." % (missing_cols, csv_reader.fieldnames))

    quest_covid_antibody_test_result_dao = QuestCovidAntibodyTestResultDao()
    records = []
    rows = list(csv_reader)
    with quest_covid_antibody_test_result_dao.session() as session:
        for row in rows:
            if not row[QuestCovidAntibodyTestResultCsvColumns.ACCESSION]:
                continue
            record = _create_quest_covid_antibody_test_result_obj_from_row(row, csv_filename)
            records.append(record)
            if len(records) >= _BATCH_SIZE:
                quest_covid_antibody_test_result_dao.upsert_all_with_session(session, records)
                records = []
        if records:
            quest_covid_antibody_test_result_dao.upsert_all_with_session(session, records)


def _create_quest_covid_antibody_test_result_obj_from_row(row, csv_filename):
    kwargs = dict(
        accession=row[QuestCovidAntibodyTestResultCsvColumns.ACCESSION],
        resultName=row[QuestCovidAntibodyTestResultCsvColumns.RESULT_NAME],
        resultValue=row[QuestCovidAntibodyTestResultCsvColumns.RESULT_VALUE],
        ingestFileName=csv_filename
    )

    obj = QuestCovidAntibodyTestResult(**kwargs)

    return obj


def _find_file_list(cloud_bucket_name, file_name_prefix=None, sub_folder_name=None, file_suffix_name='.csv'):
    """
    Returns the path and name list of the files meet the specified criteria.
    Raises:
        RuntimeError: if no files are found in the cloud storage bucket.
    """
    bucket_stat_list = list_blobs("/" + cloud_bucket_name)
    if not bucket_stat_list:
        raise FileNotFoundError("No files in cloud bucket %r." % cloud_bucket_name)

    # GCS does not really have the concept of directories (it's just a filename convention), so all
    # directory listings are recursive and we must filter out subdirectory contents.
    bucket_stat_list = [
        s
        for s in bucket_stat_list
        if s.name.lower().endswith(file_suffix_name) and (sub_folder_name is None or sub_folder_name in s.name)
    ]
    if not bucket_stat_list:
        raise FileNotFoundError("No {} files in cloud bucket {} (sub-folder: {}).".format(file_suffix_name,
                                                                                          cloud_bucket_name,
                                                                                          sub_folder_name))
    file_list = []
    bucket_stat_list.sort(key=lambda s: s.updated)
    for s in bucket_stat_list:
        if _is_fresh_file(s.updated):
            blob_name = s.name
            file_name = os.path.basename(blob_name)
            cloud_path = os.path.normpath(cloud_bucket_name + '/' + blob_name)
            if file_name_prefix and file_name_prefix in blob_name:
                file_list.append((cloud_path, file_name))
            elif file_name_prefix is None:
                file_list.append((cloud_path, file_name))

    return file_list


def _is_fresh_file(file_last_modified_time):
    days = config.getSetting(config.ANTIBODY_DATA_IMPORT_DAYS, default=1)
    max_input_age = datetime.timedelta(hours=24 * days)
    now = clock.CLOCK.now()
    timezone = pytz.timezone('Etc/Greenwich')
    now_with_timezone = timezone.localize(now)
    if now_with_timezone - file_last_modified_time > max_input_age:
        return False
    else:
        return True


class BiobankAntibodyManifestCsvColumns(object):
    BIOBANK_ID = "Biobank ID"
    SAMPLE_ID = "Sample ID"
    MATRIX_ID = "Matrix (Tube) ID"
    SAMPLE_TYPE = "Sample Type"
    QUANTITY = "Quantity (uL)"
    STORAGE_LOCATION = "Storage Location"
    COLLECTION_DATE = "Collection Date"

    ALL = (BIOBANK_ID, SAMPLE_ID, MATRIX_ID, SAMPLE_TYPE, QUANTITY, STORAGE_LOCATION, COLLECTION_DATE)


class QuestCovidAntibodyTestCsvColumns(object):
    SPECIMEN_ID = "Specimen ID"
    TEST_CODE = "Test Code"
    TEST_NAME = "Test Name"
    RUN_DATE_TIME = "Run Date Time"
    ACCESSION = "Accession"
    INSTRUMENT_NAME = "Instrument Name"
    POSITION = "Position"

    ALL = (SPECIMEN_ID, TEST_CODE, TEST_NAME, RUN_DATE_TIME, ACCESSION, INSTRUMENT_NAME, POSITION)


class QuestCovidAntibodyTestResultCsvColumns(object):
    ACCESSION = "Accession"
    RESULT_NAME = "Result Name"
    RESULT_VALUE = "Result Value"

    ALL = (ACCESSION, RESULT_NAME, RESULT_VALUE)


class DataError(RuntimeError):
    def __init__(self, msg, external=False):
        super(DataError, self).__init__(msg)
        self.external = external
