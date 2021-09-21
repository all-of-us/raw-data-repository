import csv
import datetime
import os
import pytz
import shutil  # pylint: disable=unused-import

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file
from rdr_service.code_constants import BIOBANK_TESTS
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.genomics_dao import GenomicSetDao, GenomicSetMemberDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.genomic import genomic_biobank_manifest_handler, genomic_set_file_handler
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
)
from rdr_service.genomic_enums import GenomicSetStatus, GenomicSetMemberStatus, GenomicValidationFlag, \
    GenomicWorkflowState
from rdr_service.model.participant import Participant
# from tests import test_data
from tests.helpers.unittest_base import BaseTestCase

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = "rdr_fake_bucket"
_FAKE_BUCKET_FOLDER = "rdr_fake_sub_folder"
_OUTPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
_US_CENTRAL = pytz.timezone("US/Central")
_UTC = pytz.utc


class GenomicSetFileHandlerTest(BaseTestCase):
    def setUp(self):
        super(GenomicSetFileHandlerTest, self).setUp()
        config.override_setting(config.GENOMIC_SET_BUCKET_NAME, [_FAKE_BUCKET])
        config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BUCKET])
        config.override_setting(config.GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME, [_FAKE_BUCKET_FOLDER])
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()

    mock_bucket_paths = [_FAKE_BUCKET, _FAKE_BUCKET + os.sep + _FAKE_BUCKET_FOLDER]

    @staticmethod
    def _write_cloud_csv(file_name, contents_str, bucket=None, folder=None):
        bucket = _FAKE_BUCKET if bucket is None else bucket
        if folder is None:
            path = "/%s/%s" % (bucket, file_name)
        else:
            path = "/%s/%s/%s" % (bucket, folder, file_name)
        with open_cloud_file(path, mode='wb') as cloud_file:
            cloud_file.write(contents_str.encode("utf-8"))

    @staticmethod
    def _make_biobank_order(**kwargs):
        """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
        participant_id = kwargs["participantId"]
        modified = datetime.datetime(2019, 0o3, 25, 15, 59, 30)

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
            ("mailKitOrders", [BiobankMailKitOrder(participantId=participant_id, modified=modified, version=1)]),
        ):
            if k not in kwargs:
                kwargs[k] = default_value
        return BiobankOrder(**kwargs)

    def test_no_file_found(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        # If no file found, it will not raise any error
        self.assertIsNone(genomic_set_file_handler.read_genomic_set_from_bucket())

    def test_create_and_upload_biobank_manifest_file(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        participant = self.participant_dao.insert(Participant(participantId=123, biobankId=123))
        self.summary_dao.insert(self.participant_summary(participant))
        bo = self._make_biobank_order(
            participantId=participant.participantId,
            biobankOrderId="123",
            identifiers=[BiobankOrderIdentifier(system="https://www.pmi-ops.org", value="12345678")],
        )
        BiobankOrderDao().insert(bo)

        participant2 = self.participant_dao.insert(Participant(participantId=124, biobankId=124))
        self.summary_dao.insert(self.participant_summary(participant2))
        bo2 = self._make_biobank_order(
            participantId=participant2.participantId,
            biobankOrderId="124",
            identifiers=[BiobankOrderIdentifier(system="https://www.pmi-ops.org", value="12345679")],
        )
        BiobankOrderDao().insert(bo2)

        participant3 = self.participant_dao.insert(Participant(participantId=125, biobankId=125))
        self.summary_dao.insert(self.participant_summary(participant3))
        bo3 = self._make_biobank_order(
            participantId=participant3.participantId,
            biobankOrderId="125",
            identifiers=[BiobankOrderIdentifier(system="https://www.pmi-ops.org", value="12345680")],
        )
        BiobankOrderDao().insert(bo3)

        genomic_set = self._create_fake_genomic_set(
            "fake_genomic_set_name", "fake_genomic_set_criteria", "Genomic-Test-Set-2019-04-05-00-30-10.CSV"
        )
        self._create_fake_genomic_member(
            genomic_set.id,
            participant.participantId,
            bo.biobankOrderId,
            participant.biobankId,
            bo.identifiers[0].value,
            validation_status=GenomicSetMemberStatus.VALID,
            sex_at_birth="F",
            genome_type="aou_array",
            ny_flag="Y",
        )

        self._create_fake_genomic_member(
            genomic_set.id,
            participant2.participantId,
            bo2.biobankOrderId,
            participant2.biobankId,
            bo2.identifiers[0].value,
            validation_status=GenomicSetMemberStatus.INVALID,
            validation_flags=[GenomicValidationFlag.INVALID_AGE],
            sex_at_birth="M",
            genome_type="aou_array",
            ny_flag="N",
        )

        self._create_fake_genomic_member(
            genomic_set.id,
            participant3.participantId,
            bo3.biobankOrderId,
            participant3.biobankId,
            bo3.identifiers[0].value,
            validation_status=GenomicSetMemberStatus.INVALID,
            validation_flags=[GenomicValidationFlag.INVALID_CONSENT],
            sex_at_birth="F",
            genome_type="aou_wgs",
            ny_flag="Y",
        )

        now = clock.CLOCK.now()
        genomic_biobank_manifest_handler.create_and_upload_genomic_biobank_manifest_file(genomic_set.id, now)

        bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)
        # convert UTC to CDT
        now_cdt_str = _UTC.localize(now).astimezone(_US_CENTRAL).replace(tzinfo=None).strftime(_OUTPUT_CSV_TIME_FORMAT)

        class ExpectedCsvColumns(object):
            BIOBANK_ID = "biobank_id"
            SEX_AT_BIRTH = "sex_at_birth"
            GENOME_TYPE = "genome_type"
            NY_FLAG = "ny_flag"

            ALL = (SEX_AT_BIRTH, GENOME_TYPE, NY_FLAG)

        expected_result_filename = "rdr_fake_sub_folder/Genomic-Manifest-AoU-" + now_cdt_str + "-1.csv"
        path = "/" + bucket_name + "/" + expected_result_filename
        with open_cloud_file(path) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=",")

            missing_cols = set(ExpectedCsvColumns.ALL) - set(csv_reader.fieldnames)
            self.assertEqual(len(missing_cols), 0)
            rows = list(csv_reader)
            self.assertEqual(rows[0][ExpectedCsvColumns.BIOBANK_ID], "T123")
            self.assertEqual(rows[0][ExpectedCsvColumns.SEX_AT_BIRTH], "F")
            self.assertEqual(rows[0][ExpectedCsvColumns.GENOME_TYPE], "aou_array")
            self.assertEqual(rows[0][ExpectedCsvColumns.NY_FLAG], "Y")
            self.assertEqual(rows[1][ExpectedCsvColumns.BIOBANK_ID], "T124")
            self.assertEqual(rows[1][ExpectedCsvColumns.SEX_AT_BIRTH], "M")
            self.assertEqual(rows[1][ExpectedCsvColumns.GENOME_TYPE], "aou_array")
            self.assertEqual(rows[1][ExpectedCsvColumns.NY_FLAG], "N")
            self.assertEqual(rows[2][ExpectedCsvColumns.BIOBANK_ID], "T125")
            self.assertEqual(rows[2][ExpectedCsvColumns.SEX_AT_BIRTH], "F")
            self.assertEqual(rows[2][ExpectedCsvColumns.GENOME_TYPE], "aou_wgs")
            self.assertEqual(rows[2][ExpectedCsvColumns.NY_FLAG], "Y")

    def _create_fake_genomic_set(self, genomic_set_name, genomic_set_criteria, genomic_set_filename):
        now = clock.CLOCK.now()
        genomic_set = GenomicSet()
        genomic_set.genomicSetName = genomic_set_name
        genomic_set.genomicSetCriteria = genomic_set_criteria
        genomic_set.genomicSetFile = genomic_set_filename
        genomic_set.genomicSetFileTime = now
        genomic_set.genomicSetStatus = GenomicSetStatus.INVALID

        set_dao = GenomicSetDao()
        genomic_set.genomicSetVersion = set_dao.get_new_version_number(genomic_set.genomicSetName)
        genomic_set.created = now
        genomic_set.modified = now

        set_dao.insert(genomic_set)

        return genomic_set

    def _create_fake_genomic_member(
        self,
        genomic_set_id,
        participant_id,
        biobank_order_id,
        biobank_id,
        biobank_order_client_id,
        validation_status=GenomicSetMemberStatus.VALID,
        validation_flags=None,
        sex_at_birth="F",
        genome_type="aou_array",
        ny_flag="Y",
        genomic_workflow_state=GenomicWorkflowState.AW0_READY
    ):
        now = clock.CLOCK.now()
        genomic_set_member = GenomicSetMember()
        genomic_set_member.genomicSetId = genomic_set_id
        genomic_set_member.created = now
        genomic_set_member.modified = now
        genomic_set_member.validationStatus = validation_status
        genomic_set_member.validationFlags = validation_flags
        genomic_set_member.participantId = participant_id
        genomic_set_member.sexAtBirth = sex_at_birth
        genomic_set_member.genomeType = genome_type
        genomic_set_member.nyFlag = 1 if ny_flag == "Y" else 0
        genomic_set_member.biobankOrderId = biobank_order_id
        genomic_set_member.biobankId = biobank_id
        genomic_set_member.biobankOrderClientId = biobank_order_client_id
        genomic_set_member.genomicWorkflowState = genomic_workflow_state

        member_dao = GenomicSetMemberDao()
        member_dao.insert(genomic_set_member)

    def _naive_utc_to_naive_central(self, naive_utc_date):
        utc_date = pytz.utc.localize(naive_utc_date)
        central_date = utc_date.astimezone(pytz.timezone("US/Central"))
        return central_date.replace(tzinfo=None)
