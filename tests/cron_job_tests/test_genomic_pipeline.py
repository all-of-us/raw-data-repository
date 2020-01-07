import csv
import datetime
import os
import pytz

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file, list_blobs
from rdr_service.code_constants import BIOBANK_TESTS
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.genomics_dao import (
    GenomicSetDao,
    GenomicSetMemberDao,
    GenomicJobRunDao,
    GenomicFileProcessedDao,
    GenomicGCValidationMetricsDao,
)
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.genomic import genomic_set_file_handler
from rdr_service.genomic.genomic_set_file_handler import DataError
from rdr_service.model.biobank_dv_order import BiobankDVOrder
from rdr_service.model.biobank_order import (
    BiobankOrder,
    BiobankOrderIdentifier,
    BiobankOrderedSample
)
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember
)
from rdr_service.model.participant import Participant
from rdr_service.offline import genomic_pipeline
from rdr_service.participant_enums import SampleStatus, GenomicSetStatus, GenomicSetMemberStatus, \
    GenomicSubProcessStatus, GenomicSubProcessResult
from tests import test_data
from tests.helpers.unittest_base import BaseTestCase

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = "rdr_fake_bucket"
_FAKE_BIOBANK_SAMPLE_BUCKET = "rdr_fake_biobank_sample_bucket"
_FAKE_BUCKET_FOLDER = "rdr_fake_sub_folder"
_FAKE_BUCKET_RESULT_FOLDER = "rdr_fake_sub_result_folder"
_FAKE_GENOMIC_CENTER_BUCKET_A = 'rdr_fake_genomic_center_a_bucket'
_FAKE_GENOMIC_CENTER_BUCKET_B = 'rdr_fake_genomic_center_b_bucket'
_FAKE_GENOTYPING_FOLDER = 'rdr_fake_genotyping_folder'
_OUTPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
_US_CENTRAL = pytz.timezone("US/Central")
_UTC = pytz.utc


class GenomicPipelineTest(BaseTestCase):
    def setUp(self):
        super(GenomicPipelineTest, self).setUp()
        # Everything is stored as a list, so override bucket name as a 1-element list.
        config.override_setting(config.GENOMIC_SET_BUCKET_NAME, [_FAKE_BUCKET])
        config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BIOBANK_SAMPLE_BUCKET])
        config.override_setting(config.GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME, [_FAKE_BUCKET_FOLDER])
        config.override_setting(config.GENOMIC_BIOBANK_MANIFEST_RESULT_FOLDER_NAME, [_FAKE_BUCKET_RESULT_FOLDER])
        config.override_setting(config.GENOMIC_CENTER_BUCKET_NAME, [_FAKE_GENOMIC_CENTER_BUCKET_A,
                                                                    _FAKE_GENOMIC_CENTER_BUCKET_B])
        config.override_setting(config.GENOMIC_GENOTYPING_SAMPLE_MANIFEST_FOLDER_NAME,
                                [_FAKE_GENOTYPING_FOLDER])

        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()
        self.job_run_dao = GenomicJobRunDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self._participant_i = 1

    mock_bucket_paths = [_FAKE_BUCKET,
                         _FAKE_BIOBANK_SAMPLE_BUCKET,
                         _FAKE_BIOBANK_SAMPLE_BUCKET + os.sep + _FAKE_BUCKET_FOLDER,
                         _FAKE_BIOBANK_SAMPLE_BUCKET + os.sep + _FAKE_BUCKET_RESULT_FOLDER
                         ]

    def _write_cloud_csv(self, file_name, contents_str, bucket=None, folder=None):
        bucket = _FAKE_BUCKET if bucket is None else bucket
        if folder is None:
            path = "/%s/%s" % (bucket, file_name)
        else:
            path = "/%s/%s/%s" % (bucket, folder, file_name)
        with open_cloud_file(path, mode='wb') as cloud_file:
            cloud_file.write(contents_str.encode("utf-8"))

    def _make_participant(self, **kwargs):
        """
    Make a participant with custom settings.
    default should create a valid participant.
    """
        i = self._participant_i
        self._participant_i += 1
        participant = Participant(participantId=i, biobankId=i, **kwargs)
        self.participant_dao.insert(participant)
        return participant

    def _make_biobank_order(self, **kwargs):
        """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
        participant_id = kwargs["participantId"]

        for k, default_value in (
            ("biobankOrderId", "1"),
            ("created", clock.CLOCK.now()),
            ("sourceSiteId", 1),
            ("sourceUsername", "fred@pmi-ops.org"),
            ("collectedSiteId", 1),
            ("collectedUsername", "joe@pmi-ops.org"),
            ("processedSiteId", 1),
            ("processedUsername", "sue@pmi-ops.org"),
            ("finalizedSiteId", 2),
            ("finalizedUsername", "bob@pmi-ops.org"),
            ("version", 1),
            ("identifiers", [BiobankOrderIdentifier(system="a", value="c")]),
            ("samples", [BiobankOrderedSample(test="1SAL2", description="description", processingRequired=True)]),
            ("dvOrders", [BiobankDVOrder(participantId=participant_id, version=1)]),
        ):
            if k not in kwargs:
                kwargs[k] = default_value

        biobank_order = BiobankOrderDao().insert(BiobankOrder(**kwargs))
        return biobank_order

    def _make_summary(self, participant, **override_kwargs):
        """
    Make a summary with custom settings.
    default should create a valid summary.
    """
        valid_kwargs = dict(
            participantId=participant.participantId,
            biobankId=participant.biobankId,
            withdrawalStatus=participant.withdrawalStatus,
            dateOfBirth=datetime.datetime(2000, 1, 1),
            firstName="foo",
            lastName="bar",
            zipCode="12345",
            sampleStatus1ED04=SampleStatus.RECEIVED,
            sampleStatus1SAL2=SampleStatus.RECEIVED,
            samplesToIsolateDNA=SampleStatus.RECEIVED,
            consentForStudyEnrollmentTime=datetime.datetime(2019, 1, 1),
        )
        kwargs = dict(valid_kwargs, **override_kwargs)
        summary = self._participant_summary_with_defaults(**kwargs)
        self.summary_dao.insert(summary)
        return summary

    def test_end_to_end_valid_case(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        participant = self._make_participant()
        self._make_summary(participant)
        self._make_biobank_order(
            participantId=participant.participantId,
            biobankOrderId=participant.participantId,
            identifiers=[BiobankOrderIdentifier(system="https://www.pmi-ops.org", value="12345678")],
        )

        participant2 = self._make_participant()
        self._make_summary(participant2)
        self._make_biobank_order(
            participantId=participant2.participantId,
            biobankOrderId=participant2.participantId,
            identifiers=[BiobankOrderIdentifier(system="https://www.pmi-ops.org", value="12345679")],
        )

        participant3 = self._make_participant()
        self._make_summary(participant3)
        self._make_biobank_order(
            participantId=participant3.participantId,
            biobankOrderId=participant3.participantId,
            identifiers=[BiobankOrderIdentifier(system="https://www.pmi-ops.org", value="12345680")],
        )

        samples_file = test_data.open_genomic_set_file("Genomic-Test-Set-test-2.csv")

        input_filename = "Genomic-Test-Set-v1%s.csv" % self._naive_utc_to_naive_central(clock.CLOCK.now()).strftime(
            genomic_set_file_handler.INPUT_CSV_TIME_FORMAT
        )

        self._write_cloud_csv(input_filename, samples_file)

        manifest_result_file = test_data.open_genomic_set_file("Genomic-Manifest-Result-test.csv")

        manifest_result_filename = "Genomic-Manifest-Result-AoU-1-v1%s.csv" % self._naive_utc_to_naive_central(
            clock.CLOCK.now()
        ).strftime(genomic_set_file_handler.INPUT_CSV_TIME_FORMAT)

        self._write_cloud_csv(
            manifest_result_filename,
            manifest_result_file,
            bucket=_FAKE_BIOBANK_SAMPLE_BUCKET,
            folder=_FAKE_BUCKET_RESULT_FOLDER,
        )

        genotyping_sample_manifest_file_1 = test_data.open_genomic_set_file(
            'Genomic-Test-Genotyping-Sample-Manifest-1.csv')
        genotyping_sample_manifest_filename_1 = 'CIDR_AoU_GEN_PKG-1907-120819.csv'
        self._write_cloud_csv(genotyping_sample_manifest_filename_1, genotyping_sample_manifest_file_1,
                              bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
                              folder=_FAKE_GENOTYPING_FOLDER)

        genotyping_sample_manifest_file_2 = test_data.open_genomic_set_file(
            'Genomic-Test-Genotyping-Sample-Manifest-2.csv')
        genotyping_sample_manifest_filename_2 = 'CIDR_AoU_GEN_PKG-1907-120810.csv'
        self._write_cloud_csv(genotyping_sample_manifest_filename_2, genotyping_sample_manifest_file_2,
                              bucket=_FAKE_GENOMIC_CENTER_BUCKET_B,
                              folder=_FAKE_GENOTYPING_FOLDER)

        genomic_pipeline.process_genomic_water_line()

        # verify result file
        bucket_name = config.getSetting(config.GENOMIC_SET_BUCKET_NAME)
        blob_name = self._find_latest_genomic_set_csv(bucket_name, "Validation-Result")
        with open_cloud_file(os.path.normpath(bucket_name + '/' + blob_name)) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=",")

            class ResultCsvColumns(object):
                """Names of CSV columns that we read from the genomic set upload."""

                GENOMIC_SET_NAME = "genomic_set_name"
                GENOMIC_SET_CRITERIA = "genomic_set_criteria"
                PID = "pid"
                BIOBANK_ORDER_ID = "biobank_order_id"
                NY_FLAG = "ny_flag"
                SEX_AT_BIRTH = "sex_at_birth"
                GENOME_TYPE = "genome_type"
                STATUS = "status"
                INVALID_REASON = "invalid_reason"

                ALL = (
                    GENOMIC_SET_NAME,
                    GENOMIC_SET_CRITERIA,
                    PID,
                    BIOBANK_ORDER_ID,
                    NY_FLAG,
                    SEX_AT_BIRTH,
                    GENOME_TYPE,
                    STATUS,
                    INVALID_REASON,
                )

            missing_cols = set(ResultCsvColumns.ALL) - set(csv_reader.fieldnames)
            self.assertEqual(len(missing_cols), 0)
            rows = list(csv_reader)
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0][ResultCsvColumns.GENOMIC_SET_NAME], "name_xxx")
            self.assertEqual(rows[0][ResultCsvColumns.GENOMIC_SET_CRITERIA], "criteria_xxx")
            self.assertEqual(rows[0][ResultCsvColumns.STATUS], "valid")
            self.assertEqual(rows[0][ResultCsvColumns.INVALID_REASON], "")
            self.assertEqual(rows[0][ResultCsvColumns.PID], "1")
            self.assertEqual(rows[0][ResultCsvColumns.BIOBANK_ORDER_ID], "1")
            self.assertEqual(rows[0][ResultCsvColumns.NY_FLAG], "Y")
            self.assertEqual(rows[0][ResultCsvColumns.GENOME_TYPE], "aou_wgs")
            self.assertEqual(rows[0][ResultCsvColumns.SEX_AT_BIRTH], "M")

            self.assertEqual(rows[1][ResultCsvColumns.GENOMIC_SET_NAME], "name_xxx")
            self.assertEqual(rows[1][ResultCsvColumns.GENOMIC_SET_CRITERIA], "criteria_xxx")
            self.assertEqual(rows[1][ResultCsvColumns.STATUS], "valid")
            self.assertEqual(rows[1][ResultCsvColumns.INVALID_REASON], "")
            self.assertEqual(rows[1][ResultCsvColumns.PID], "2")
            self.assertEqual(rows[1][ResultCsvColumns.BIOBANK_ORDER_ID], "2")
            self.assertEqual(rows[1][ResultCsvColumns.NY_FLAG], "N")
            self.assertEqual(rows[1][ResultCsvColumns.GENOME_TYPE], "aou_array")
            self.assertEqual(rows[1][ResultCsvColumns.SEX_AT_BIRTH], "F")

            self.assertEqual(rows[2][ResultCsvColumns.GENOMIC_SET_NAME], "name_xxx")
            self.assertEqual(rows[2][ResultCsvColumns.GENOMIC_SET_CRITERIA], "criteria_xxx")
            self.assertEqual(rows[2][ResultCsvColumns.STATUS], "valid")
            self.assertEqual(rows[2][ResultCsvColumns.INVALID_REASON], "")
            self.assertEqual(rows[2][ResultCsvColumns.PID], "3")
            self.assertEqual(rows[2][ResultCsvColumns.BIOBANK_ORDER_ID], "3")
            self.assertEqual(rows[2][ResultCsvColumns.NY_FLAG], "N")
            self.assertEqual(rows[2][ResultCsvColumns.GENOME_TYPE], "aou_array")
            self.assertEqual(rows[2][ResultCsvColumns.SEX_AT_BIRTH], "M")

        # verify manifest files
        bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)

        class ExpectedCsvColumns(object):
            VALUE = "value"
            BIOBANK_ID = "biobank_id"
            SEX_AT_BIRTH = "sex_at_birth"
            GENOME_TYPE = "genome_type"
            NY_FLAG = "ny_flag"
            REQUEST_ID = "request_id"
            PACKAGE_ID = "package_id"

            ALL = (VALUE, SEX_AT_BIRTH, GENOME_TYPE, NY_FLAG, REQUEST_ID, PACKAGE_ID)

        blob_name = self._find_latest_genomic_set_csv(bucket_name, _FAKE_BUCKET_FOLDER)
        with open_cloud_file(os.path.normpath(bucket_name + '/' + blob_name)) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=",")

            missing_cols = set(ExpectedCsvColumns.ALL) - set(csv_reader.fieldnames)
            self.assertEqual(len(missing_cols), 0)
            rows = list(csv_reader)
            self.assertEqual(rows[0][ExpectedCsvColumns.VALUE], "")
            self.assertEqual(rows[0][ExpectedCsvColumns.BIOBANK_ID], "T1")
            self.assertEqual(rows[0][ExpectedCsvColumns.SEX_AT_BIRTH], "M")
            self.assertEqual(rows[0][ExpectedCsvColumns.GENOME_TYPE], "aou_wgs")
            self.assertEqual(rows[0][ExpectedCsvColumns.NY_FLAG], "Y")
            self.assertEqual(rows[1][ExpectedCsvColumns.VALUE], "")
            self.assertEqual(rows[1][ExpectedCsvColumns.BIOBANK_ID], "T2")
            self.assertEqual(rows[1][ExpectedCsvColumns.SEX_AT_BIRTH], "F")
            self.assertEqual(rows[1][ExpectedCsvColumns.GENOME_TYPE], "aou_array")
            self.assertEqual(rows[1][ExpectedCsvColumns.NY_FLAG], "N")
            self.assertEqual(rows[2][ExpectedCsvColumns.VALUE], "")
            self.assertEqual(rows[2][ExpectedCsvColumns.BIOBANK_ID], "T3")
            self.assertEqual(rows[2][ExpectedCsvColumns.SEX_AT_BIRTH], "M")
            self.assertEqual(rows[2][ExpectedCsvColumns.GENOME_TYPE], "aou_array")
            self.assertEqual(rows[2][ExpectedCsvColumns.NY_FLAG], "N")

        # verify manifest result files
        bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)

        class ExpectedCsvColumns(object):
            VALUE = "value"
            BIOBANK_ID = "biobank_id"
            SEX_AT_BIRTH = "sex_at_birth"
            GENOME_TYPE = "genome_type"
            NY_FLAG = "ny_flag"
            REQUEST_ID = "request_id"
            PACKAGE_ID = "package_id"

            ALL = (VALUE, SEX_AT_BIRTH, GENOME_TYPE, NY_FLAG, REQUEST_ID, PACKAGE_ID)

        blob_name = self._find_latest_genomic_set_csv(bucket_name, _FAKE_BUCKET_RESULT_FOLDER)
        with open_cloud_file(os.path.normpath(bucket_name + '/' + blob_name)) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=",")

            missing_cols = set(ExpectedCsvColumns.ALL) - set(csv_reader.fieldnames)
            self.assertEqual(len(missing_cols), 0)
            rows = list(csv_reader)
            self.assertEqual(rows[0][ExpectedCsvColumns.VALUE], "12345678")
            self.assertEqual(rows[0][ExpectedCsvColumns.BIOBANK_ID], "T1")
            self.assertEqual(rows[0][ExpectedCsvColumns.SEX_AT_BIRTH], "M")
            self.assertEqual(rows[0][ExpectedCsvColumns.GENOME_TYPE], "aou_wgs")
            self.assertEqual(rows[0][ExpectedCsvColumns.NY_FLAG], "Y")
            self.assertEqual(rows[0][ExpectedCsvColumns.PACKAGE_ID], "PKG-XXXX-XXXX1")

            self.assertEqual(rows[1][ExpectedCsvColumns.VALUE], "12345679")
            self.assertEqual(rows[1][ExpectedCsvColumns.BIOBANK_ID], "T2")
            self.assertEqual(rows[1][ExpectedCsvColumns.SEX_AT_BIRTH], "F")
            self.assertEqual(rows[1][ExpectedCsvColumns.GENOME_TYPE], "aou_array")
            self.assertEqual(rows[1][ExpectedCsvColumns.NY_FLAG], "N")
            self.assertEqual(rows[1][ExpectedCsvColumns.PACKAGE_ID], "PKG-XXXX-XXXX2")

            self.assertEqual(rows[2][ExpectedCsvColumns.VALUE], "12345680")
            self.assertEqual(rows[2][ExpectedCsvColumns.BIOBANK_ID], "T3")
            self.assertEqual(rows[2][ExpectedCsvColumns.SEX_AT_BIRTH], "M")
            self.assertEqual(rows[2][ExpectedCsvColumns.GENOME_TYPE], "aou_array")
            self.assertEqual(rows[2][ExpectedCsvColumns.NY_FLAG], "N")
            self.assertEqual(rows[2][ExpectedCsvColumns.PACKAGE_ID], "PKG-XXXX-XXXX3")

        # verify package id and sample id in database
        member_dao = GenomicSetMemberDao()
        members = member_dao.get_all()
        for member in members:
            self.assertIn(member.packageId, ["PKG-XXXX-XXXX1", "PKG-XXXX-XXXX2", "PKG-XXXX-XXXX3"])
            self.assertIn(member.biobankOrderClientId, ["12345678", "12345679", "12345680"])
            self.assertIn(member.sampleId, ['19224001502', '19224001510', '19224001518'])
            self.assertIn(member.sampleType, ['DNA'])

    def test_wrong_file_name_case(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        samples_file = test_data.open_genomic_set_file("Genomic-Test-Set-test-3.csv")

        input_filename = "Genomic-Test-Set-v1%swrong-name.csv" % self._naive_utc_to_naive_central(
            clock.CLOCK.now()
        ).strftime(genomic_set_file_handler.INPUT_CSV_TIME_FORMAT)

        self._write_cloud_csv(input_filename, samples_file)

        with self.assertRaises(DataError):
            genomic_pipeline.process_genomic_water_line()

        manifest_result_file = test_data.open_genomic_set_file("Genomic-Manifest-Result-test.csv")

        manifest_result_filename = "Genomic-Manifest-Result-AoU-1-v1%swrong-name.csv" % self._naive_utc_to_naive_central(
            clock.CLOCK.now()
        ).strftime(
            genomic_set_file_handler.INPUT_CSV_TIME_FORMAT
        )

        self._write_cloud_csv(
            manifest_result_filename,
            manifest_result_file,
            bucket=_FAKE_BIOBANK_SAMPLE_BUCKET,
            folder=_FAKE_BUCKET_RESULT_FOLDER,
        )

        with self.assertRaises(DataError):
            genomic_pipeline.process_genomic_water_line()

    def test_over_24hours_genomic_set_file_case(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        samples_file = test_data.open_genomic_set_file("Genomic-Test-Set-test-3.csv")

        over_24hours_time = clock.CLOCK.now() - datetime.timedelta(hours=25)

        input_filename = "Genomic-Test-Set-v1%s.csv" % self._naive_utc_to_naive_central(over_24hours_time).strftime(
            genomic_set_file_handler.INPUT_CSV_TIME_FORMAT
        )

        self._write_cloud_csv(input_filename, samples_file)

        genomic_pipeline.process_genomic_water_line()

        member_dao = GenomicSetMemberDao()
        members = member_dao.get_all()
        self.assertEqual(len(members), 0)

    def test_end_to_end_invalid_case(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        participant = self._make_participant()
        self._make_summary(participant, dateOfBirth="2018-02-14", zipCode="")
        self._make_biobank_order(
            participantId=participant.participantId,
            biobankOrderId=participant.participantId,
            identifiers=[BiobankOrderIdentifier(system="https://www.pmi-ops.org", value="12345678")],
        )

        participant2 = self._make_participant()
        self._make_summary(participant2, consentForStudyEnrollmentTime=datetime.datetime(1990, 1, 1))
        self._make_biobank_order(
            participantId=participant2.participantId,
            biobankOrderId=participant2.participantId,
            identifiers=[BiobankOrderIdentifier(system="https://www.pmi-ops.org", value="12345679")],
        )

        participant3 = self._make_participant()
        self._make_summary(participant3, zipCode="")
        self._make_biobank_order(
            participantId=participant3.participantId,
            biobankOrderId=participant3.participantId,
            identifiers=[BiobankOrderIdentifier(system="https://www.pmi-ops.org", value="12345680")],
        )

        participant4 = self._make_participant()
        self._make_summary(participant4)
        self._make_biobank_order(
            participantId=participant4.participantId,
            biobankOrderId=participant4.participantId,
            identifiers=[BiobankOrderIdentifier(system="c", value="e")],
        )

        samples_file = test_data.open_genomic_set_file("Genomic-Test-Set-test-3.csv")

        input_filename = "Genomic-Test-Set-v1%s.csv" % self._naive_utc_to_naive_central(clock.CLOCK.now()).strftime(
            genomic_set_file_handler.INPUT_CSV_TIME_FORMAT
        )

        self._write_cloud_csv(input_filename, samples_file)

        genomic_pipeline.process_genomic_water_line()

        # verify result file
        bucket_name = config.getSetting(config.GENOMIC_SET_BUCKET_NAME)
        blob_name = self._find_latest_genomic_set_csv(bucket_name, "Validation-Result")
        with open_cloud_file(os.path.normpath(bucket_name + '/' + blob_name)) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=",")

            class ResultCsvColumns(object):
                """Names of CSV columns that we read from the genomic set upload."""

                GENOMIC_SET_NAME = "genomic_set_name"
                GENOMIC_SET_CRITERIA = "genomic_set_criteria"
                PID = "pid"
                BIOBANK_ORDER_ID = "biobank_order_id"
                NY_FLAG = "ny_flag"
                SEX_AT_BIRTH = "sex_at_birth"
                GENOME_TYPE = "genome_type"
                STATUS = "status"
                INVALID_REASON = "invalid_reason"

                ALL = (
                    GENOMIC_SET_NAME,
                    GENOMIC_SET_CRITERIA,
                    PID,
                    BIOBANK_ORDER_ID,
                    NY_FLAG,
                    SEX_AT_BIRTH,
                    GENOME_TYPE,
                    STATUS,
                    INVALID_REASON,
                )

            missing_cols = set(ResultCsvColumns.ALL) - set(csv_reader.fieldnames)
            self.assertEqual(len(missing_cols), 0)
            rows = list(csv_reader)
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows[0][ResultCsvColumns.GENOMIC_SET_NAME], "name_xxx")
            self.assertEqual(rows[0][ResultCsvColumns.GENOMIC_SET_CRITERIA], "criteria_xxx")
            self.assertEqual(rows[0][ResultCsvColumns.STATUS], "invalid")
            self.assertEqual(rows[0][ResultCsvColumns.INVALID_REASON], "INVALID_AGE, INVALID_NY_ZIPCODE")
            self.assertEqual(rows[0][ResultCsvColumns.PID], "1")
            self.assertEqual(rows[0][ResultCsvColumns.BIOBANK_ORDER_ID], "1")
            self.assertEqual(rows[0][ResultCsvColumns.NY_FLAG], "Y")
            self.assertEqual(rows[0][ResultCsvColumns.GENOME_TYPE], "aou_wgs")
            self.assertEqual(rows[0][ResultCsvColumns.SEX_AT_BIRTH], "M")

            self.assertEqual(rows[1][ResultCsvColumns.GENOMIC_SET_NAME], "name_xxx")
            self.assertEqual(rows[1][ResultCsvColumns.GENOMIC_SET_CRITERIA], "criteria_xxx")
            self.assertEqual(rows[1][ResultCsvColumns.STATUS], "invalid")
            self.assertEqual(rows[1][ResultCsvColumns.INVALID_REASON], "INVALID_CONSENT")
            self.assertEqual(rows[1][ResultCsvColumns.PID], "2")
            self.assertEqual(rows[1][ResultCsvColumns.BIOBANK_ORDER_ID], "2")
            self.assertEqual(rows[1][ResultCsvColumns.NY_FLAG], "N")
            self.assertEqual(rows[1][ResultCsvColumns.GENOME_TYPE], "aou_array")
            self.assertEqual(rows[1][ResultCsvColumns.SEX_AT_BIRTH], "F")

            self.assertEqual(rows[2][ResultCsvColumns.GENOMIC_SET_NAME], "name_xxx")
            self.assertEqual(rows[2][ResultCsvColumns.GENOMIC_SET_CRITERIA], "criteria_xxx")
            self.assertEqual(rows[2][ResultCsvColumns.STATUS], "invalid")
            self.assertEqual(rows[2][ResultCsvColumns.INVALID_REASON], "INVALID_NY_ZIPCODE")
            self.assertEqual(rows[2][ResultCsvColumns.PID], "3")
            self.assertEqual(rows[2][ResultCsvColumns.BIOBANK_ORDER_ID], "3")
            self.assertEqual(rows[2][ResultCsvColumns.NY_FLAG], "N")
            self.assertEqual(rows[2][ResultCsvColumns.GENOME_TYPE], "aou_array")
            self.assertEqual(rows[2][ResultCsvColumns.SEX_AT_BIRTH], "M")

    def test_gc_validation_metrics_end_to_end(self):
        # Create the fake Google Cloud CSV files to ingest
        bucket_name = config.getSetting(config.GENOMIC_GC_METRICS_BUCKET_NAME)
        end_to_end_test_files = (
            'GC_AoU_GEN_TestDataManifest.csv',
            'GC_AoU_SEQ_TestDataManifest.csv',
            'test_empty_wells.csv'
        )
        for test_file in end_to_end_test_files:
            self._create_ingestion_test_file(test_file, bucket_name)

        # run the GC Metrics Ingestion workflow
        genomic_pipeline.ingest_genomic_centers_metrics_files()

        # test file processing queue
        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(files_processed), 2)
        self._gc_files_processed_test_cases(files_processed, bucket_name)

        # Test the fields against the DB
        gc_metrics = self.metrics_dao.get_all()

        self.assertEqual(len(gc_metrics), 2)
        self._gc_metrics_ingested_data_test_cases(gc_metrics)

        # Test successful run result
        run_obj = self.job_run_dao.get(1)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def _gc_files_processed_test_cases(self, files_processed, bucket_name):
        """ sub tests for the GC Metrics end to end test """

        # Test files were moved to archive OK
        bucket_list = list_blobs('/' + bucket_name)
        archive_files = [s.name.split('/')[1] for s in bucket_list
                         if s.name.lower().startswith('processed_by_rdr')]
        bucket_files = [s.name for s in bucket_list
                        if s.name.lower().endswith('.csv')]

        for f in files_processed:
            if "SEQ" in f.fileName:
                self.assertEqual(
                    f.fileName,
                    'GC_AoU_SEQ_TestDataManifest_11192019.csv'
                )
                self.assertEqual(
                    f.filePath,
                    '/dev_genomics_cell_line_validation/'
                    'GC_AoU_SEQ_TestDataManifest_11192019.csv'
                )
            else:
                self.assertEqual(
                    f.fileName,
                    'GC_AoU_GEN_TestDataManifest_11192019.csv'
                )
                self.assertEqual(
                    f.filePath,
                    '/dev_genomics_cell_line_validation/'
                    'GC_AoU_GEN_TestDataManifest_11192019.csv'
                )

            self.assertEqual(f.fileStatus,
                             GenomicSubProcessStatus.COMPLETED)
            self.assertEqual(f.fileResult,
                             GenomicSubProcessResult.SUCCESS)

            self.assertNotIn(f.fileName, bucket_files)
            self.assertIn(f.fileName, archive_files)

    def _gc_metrics_ingested_data_test_cases(self, gc_metrics):
        """Sub tests for the end-to-end metrics test"""
        for record in gc_metrics:
            if record.biobankId == '2':
                # test SEQ File inserted correctly
                self.assertEqual('1002', record.sampleId)
                self.assertEqual('10002', record.limsId)
                self.assertEqual(2, record.meanCoverage)
                self.assertEqual(2, record.genomeCoverage)
                self.assertEqual(3, record.contamination)
                self.assertEqual('True', record.sexConcordance)
                self.assertEqual(4, record.alignedQ20Bases)
                self.assertEqual('Pass', record.processingStatus)
                self.assertEqual('This sample passed', record.notes)
                self.assertEqual('Y', record.consentForRor)
                self.assertEqual(1, record.withdrawnStatus)
                self.assertEqual(11002, record.siteId)
            else:
                # Test GEN file data inserted correctly
                self.assertEqual('1', record.biobankId)
                self.assertEqual('1001', record.sampleId)
                self.assertEqual('10001', record.limsId)
                self.assertEqual(1, record.callRate)
                self.assertEqual(4, record.contamination)
                self.assertEqual('True', record.sexConcordance)
                self.assertEqual('Pass', record.processingStatus)
                self.assertEqual('This sample passed', record.notes)
                self.assertEqual(11001, record.siteId)

    def test_gc_metrics_ingestion_bad_files(self):
        # Create the fake Google Cloud CSV files to ingest
        bucket_name = config.getSetting(config.GENOMIC_GC_METRICS_BUCKET_NAME)
        end_to_end_test_files = (
            'GC_AoU_SEQ_TestBadStructureDataManifest.csv',
            'GC-AoU-TestBadFilename-DataManifest.csv',
            'test_empty_wells.csv'
        )
        for test_file in end_to_end_test_files:
            self._create_ingestion_test_file(test_file, bucket_name)

        # run the GC Metrics Ingestion workflow
        genomic_pipeline.ingest_genomic_centers_metrics_files()

        # test file processing queue
        files_processed = self.file_processed_dao.get_all()

        # Test bad filename, invalid columns
        for f in files_processed:
            if "TestBadFilename" in f.fileName:
                self.assertEqual(f.fileResult,
                                 GenomicSubProcessResult.INVALID_FILE_NAME)
            if "TestBadStructure" in f.fileName:
                self.assertEqual(f.fileResult,
                                 GenomicSubProcessResult.INVALID_FILE_STRUCTURE)
        # Test Unsuccessful run
        run_obj = self.job_run_dao.get(1)
        self.assertEqual(GenomicSubProcessResult.ERROR, run_obj.runResult)

    def test_gc_metrics_ingestion_no_files(self):
        # run the GC Metrics Ingestion workflow
        genomic_pipeline.ingest_genomic_centers_metrics_files()

        # Test Unsuccessful run
        run_obj = self.job_run_dao.get(1)
        self.assertEqual(GenomicSubProcessResult.NO_FILES, run_obj.runResult)

    def _create_ingestion_test_file(self,
                                    test_data_filename,
                                    bucket_name):
        test_data_file = test_data.open_genomic_set_file(test_data_filename)

        input_filename = '{}{}.csv'.format(
            test_data_filename.replace('.csv', ''),
            '_11192019'
        )

        self._write_cloud_csv(input_filename,
                              test_data_file,
                              bucket=bucket_name)

    def _create_fake_genomic_set(self,
                                 genomic_set_name,
                                 genomic_set_criteria,
                                 genomic_set_filename
                                 ):
        now = clock.CLOCK.now()
        genomic_set = GenomicSet()
        genomic_set.genomicSetName = genomic_set_name
        genomic_set.genomicSetCriteria = genomic_set_criteria
        genomic_set.genomicSetFile = genomic_set_filename
        genomic_set.genomicSetFileTime = now
        genomic_set.genomicSetStatus = GenomicSetStatus.INVALID

        set_dao = GenomicSetDao()
        genomic_set.genomicSetVersion = set_dao.get_new_version_number(genomic_set.genomicSetName)

        set_dao.insert(genomic_set)

        return genomic_set

    def _create_fake_genomic_member(
        self,
        genomic_set_id,
        participant_id,
        biobank_order_id,
        validation_status=GenomicSetMemberStatus.VALID,
        validation_flags=None,
        sex_at_birth="F",
        biobankId=None,
        genome_type="aou_array",
        ny_flag="Y",
    ):
        genomic_set_member = GenomicSetMember()
        genomic_set_member.genomicSetId = genomic_set_id
        genomic_set_member.validationStatus = validation_status
        genomic_set_member.validationFlags = validation_flags
        genomic_set_member.participantId = participant_id
        genomic_set_member.sexAtBirth = sex_at_birth
        genomic_set_member.biobankId = biobankId
        genomic_set_member.genomeType = genome_type
        genomic_set_member.nyFlag = 1 if ny_flag == "Y" else 0
        genomic_set_member.biobankOrderId = biobank_order_id

        member_dao = GenomicSetMemberDao()
        member_dao.insert(genomic_set_member)

    def _naive_utc_to_naive_central(self, naive_utc_date):
        utc_date = pytz.utc.localize(naive_utc_date)
        central_date = utc_date.astimezone(pytz.timezone("US/Central"))
        return central_date.replace(tzinfo=None)

    def _find_latest_genomic_set_csv(self, cloud_bucket_name, keyword=None):
        bucket_stat_list = list_blobs(cloud_bucket_name)
        if not bucket_stat_list:
            raise RuntimeError("No files in cloud bucket %r." % cloud_bucket_name)
        bucket_stat_list = [s for s in bucket_stat_list if s.name.lower().endswith(".csv")]
        if not bucket_stat_list:
            raise RuntimeError("No CSVs in cloud bucket %r (all files: %s)." % (cloud_bucket_name, bucket_stat_list))
        if keyword:
            buckt_stat_keyword_list = []
            for item in bucket_stat_list:
                if keyword in item.name:
                    buckt_stat_keyword_list.append(item)
            if buckt_stat_keyword_list:
                buckt_stat_keyword_list.sort(key=lambda s: s.updated)
                return buckt_stat_keyword_list[-1].name
            else:
                raise RuntimeError(
                    "No CSVs in cloud bucket %r with keyword %s (all files: %s)."
                    % (cloud_bucket_name, keyword, bucket_stat_list)
                )
        bucket_stat_list.sort(key=lambda s: s.updated)
        return bucket_stat_list[-1].name

    def _create_fake_datasets_for_gc_tests(self, count):
        # fake genomic_set
        genomic_test_set = self._create_fake_genomic_set(
            genomic_set_name="genomic-test-set-cell-line",
            genomic_set_criteria=".",
            genomic_set_filename="genomic-test-set-cell-line.csv"
        )
        # make necessary fake participant data
        for p in range(1, count + 1):
            participant = self._make_participant()
            self._make_summary(participant)
            biobank_order = self._make_biobank_order(
                participantId=participant.participantId,
                biobankOrderId=p,
                identifiers=[BiobankOrderIdentifier(
                    system=u'c', value=u'e{}'.format(
                        participant.participantId))]
            )

            # Fake genomic set members.
            self._create_fake_genomic_member(
                genomic_set_id=genomic_test_set.id,
                participant_id=participant.participantId,
                biobank_order_id=biobank_order.biobankOrderId,
                validation_status=GenomicSetMemberStatus.VALID,
                validation_flags=None,
                biobankId=f'{p}',
                sex_at_birth='F', genome_type='aou_array', ny_flag='Y'
            )

    def test_gc_metrics_reconciliation_vs_manifest(self):
        # Create the fake Google Cloud CSV files to ingest
        self._create_fake_datasets_for_gc_tests(1)
        bucket_name = config.getSetting(config.GENOMIC_GC_METRICS_BUCKET_NAME)
        self._create_ingestion_test_file('GC_AoU_GEN_TestDataManifest.csv',
                                         bucket_name)

        # Run the GC Metrics Ingestion workflow
        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1
        test_set_member = self.member_dao.get(1)

        # Run the GC Metrics Reconciliation
        genomic_pipeline.reconcile_metrics_vs_manifest()  # run_id = 2
        gc_metric_record = self.metrics_dao.get(1)

        # Test the gc_metrics were updated with reconciliation data
        self.assertEqual(test_set_member.biobankId, gc_metric_record.biobankId)
        self.assertEqual(test_set_member.id, gc_metric_record.genomicSetMemberId)
        self.assertEqual(2, gc_metric_record.reconcileManifestJobRunId)

        run_obj = self.job_run_dao.get(2)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_gc_metrics_reconciliation_vs_sequencing_end_to_end(self):
        # Create the fake ingested data
        self._create_fake_datasets_for_gc_tests(1)
        bucket_name = config.getSetting(config.GENOMIC_GC_METRICS_BUCKET_NAME)
        self._create_ingestion_test_file('GC_AoU_SEQ_TestDataManifest.csv',
                                         bucket_name)
        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1

        # Test the reconciliation process
        # TODO: naming convention TBD
        sequencing_test_files = (
            'GC_sequencing_T2.txt',
            'GC_sequencing_T3.txt',
            'GC_bad_name.txt'
        )
        for f in sequencing_test_files:
            self._write_cloud_csv(f, 'attagc', bucket=bucket_name)

        genomic_pipeline.reconcile_metrics_vs_sequencing()  # run_id = 2

        gc_record = self.metrics_dao.get(1)

        # Test the gc_metrics were updated with reconciliation data
        self.assertEqual('GC_sequencing_T2.txt'
                         , gc_record.sequencingFileName)
        self.assertEqual(2, gc_record.reconcileSequencingJobRunId)

        # Test files were moved to archive OK
        bucket_list = list(list_blobs('/' + bucket_name))
        archive_files = [s.name for s in bucket_list
                         if s.name.lower().startswith(
                            config.GENOMIC_GC_PROCESSED_FOLDER_NAME)]
        bucket_files = [s.name for s in bucket_list
                        if s.name.lower().endswith('.txt')]

        for test_file in sequencing_test_files:
            # Test cases
            if test_file == 'GC_bad_name.txt' or test_file == 'GC_sequencing_T3.txt':
                # test bad sequence file name or no gc_metrics is ignored
                self.assertIn(test_file, bucket_files)
            else:
                # test the reconciled data files were moved
                self.assertNotIn(test_file, bucket_files)
                self.assertIn(f'{config.GENOMIC_GC_PROCESSED_FOLDER_NAME}/{test_file}',
                              archive_files)

        run_obj = self.job_run_dao.get(2)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_sequencing_reconciliation_no_files(self):
        # Create the fake ingested data
        self._create_fake_datasets_for_gc_tests(5)
        bucket_name = config.getSetting(config.GENOMIC_GC_METRICS_BUCKET_NAME)
        self._create_ingestion_test_file('GC_AoU_SEQ_TestDataManifest.csv',
                                         bucket_name)
        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1

        genomic_pipeline.reconcile_metrics_vs_sequencing()  # run_id = 2

        run_obj = self.job_run_dao.get(2)

        self.assertEqual(GenomicSubProcessResult.NO_FILES, run_obj.runResult)

    def test_duplicate_sequencing_reconciliation_file(self):
        # Create the fake ingested data
        self._create_fake_datasets_for_gc_tests(1)
        bucket_name = config.getSetting(config.GENOMIC_GC_METRICS_BUCKET_NAME)
        self._create_ingestion_test_file('GC_AoU_SEQ_TestDataManifest.csv',
                                         bucket_name)
        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1

        # Test file
        test_file = 'GC_sequencing_T2.txt'
        self._write_cloud_csv(test_file, 'attagc', bucket=bucket_name)

        genomic_pipeline.reconcile_metrics_vs_sequencing()  # run_id = 2

        self._write_cloud_csv(test_file, 'attagc', bucket=bucket_name)

        genomic_pipeline.reconcile_metrics_vs_sequencing()  # run_id = 3

        # Test files were moved to archive OK
        bucket_list = list(list_blobs('/' + bucket_name))
        archive_files = [s.name for s in bucket_list
                         if s.name.lower().startswith(
                config.GENOMIC_GC_PROCESSED_FOLDER_NAME)]
        bucket_files = [s.name for s in bucket_list
                        if s.name.lower().endswith('.txt')]

        # test the reconciled data files were moved
        self.assertNotIn(test_file, bucket_files)
        self.assertIn(f'{config.GENOMIC_GC_PROCESSED_FOLDER_NAME}/{test_file}',
                      archive_files)

        # Test the filename was updated
        gc_metric_record = self.metrics_dao.get(1)
        self.assertEqual(test_file, gc_metric_record.sequencingFileName)

        run_obj = self.job_run_dao.get(3)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_new_participant_workflow(self):
        pass
