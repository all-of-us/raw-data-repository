import csv
from decimal import Decimal
import io
from datetime import datetime, timedelta
import mock
import random
import time
import os
import pytz

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file
from rdr_service.code_constants import BIOBANK_TESTS, PPI_SYSTEM, RACE_QUESTION_CODE, RACE_AIAN_CODE
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.biobank_dv_order import BiobankDVOrder
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.config_utils import get_biobank_id_prefix, to_client_biobank_id
from rdr_service.model.participant import Participant
from rdr_service.offline import biobank_samples_pipeline
from rdr_service.participant_enums import SampleStatus, get_sample_status_enum_value
from tests import test_data
from tests.helpers.unittest_base import BaseTestCase

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = "rdr_fake_bucket"

_MAYO_KIT_SYSTEM = 'https://orders.mayomedicallaboratories.com/kit-id'


class BiobankSamplesPipelineTest(BaseTestCase):
    def setUp(self):
        super(BiobankSamplesPipelineTest, self).setUp()
        config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, _BASELINE_TESTS)
        # Everything is stored as a list, so override bucket name as a 1-element list.
        config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BUCKET])
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()

    mock_bucket_paths = [_FAKE_BUCKET, _FAKE_BUCKET + os.sep + biobank_samples_pipeline._REPORT_SUBDIR]

    def _write_cloud_csv(self, file_name, contents_str):
        with open_cloud_file("/%s/%s" % (_FAKE_BUCKET, file_name), mode='wb') as cloud_file:
            cloud_file.write(contents_str.encode("utf-8"))

    def _make_biobank_order(self, **kwargs):
        """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
        participantId = kwargs["participantId"]
        modified = datetime(2019, 0o3, 25, 15, 59, 30)

        for k, default_value in (
            ("biobankOrderId", "1"),
            ("created", clock.CLOCK.now()),
            # ('participantId', self.participant.participantId),
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
            ("dvOrders", [BiobankDVOrder(participantId=participantId, modified=modified, version=1)]),
        ):
            if k not in kwargs:
                kwargs[k] = default_value
        return BiobankOrder(**kwargs)

    def test_dv_order_sample_update(self):
        """
        Test Biobank Direct Volunteer order
        """
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        participant = self.participant_dao.insert(Participant())
        self.summary_dao.insert(self.participant_summary(participant))

        created_ts = datetime(2019, 0o3, 22, 18, 30, 45)
        confirmed_ts = datetime(2019, 0o3, 23, 12, 13, 00)

        bo = self._make_biobank_order(participantId=participant.participantId)
        BiobankOrderDao().insert(bo)

        boi = bo.identifiers[0]

        bss = BiobankStoredSample(
            biobankStoredSampleId="23523523",
            biobankId=participant.biobankId,
            test="1SAL2",
            created=created_ts,
            biobankOrderIdentifier=boi.value,
            confirmed=confirmed_ts,
        )

        with self.participant_dao.session() as session:
            session.add(bss)

        ps = self.summary_dao.get(participant.participantId)
        self.assertIsNone(ps.sampleStatusDV1SAL2)
        self.assertIsNone(ps.sampleStatusDV1SAL2Time)

        self.summary_dao.update_from_biobank_stored_samples()
        ps = self.summary_dao.get(participant.participantId)
        self.assertEqual(ps.sampleStatus1SAL2, SampleStatus.RECEIVED)
        self.assertEqual(ps.sampleStatus1SAL2Time, confirmed_ts)

    def test_end_to_end(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        dao = BiobankStoredSampleDao()
        self.assertEqual(dao.count(), 0)

        # Create 3 participants and pass their (random) IDs into sample rows.
        summary_dao = ParticipantSummaryDao()
        biobank_ids = []
        participant_ids = []
        nids = 16  # equal to the number of parent rows in 'biobank_samples_1.csv'
        cids = 1  # equal to the number of child rows in 'biobank_samples_1.csv'

        for _ in range(nids):
            participant = self.participant_dao.insert(Participant())
            summary_dao.insert(self.participant_summary(participant))
            participant_ids.append(participant.participantId)
            biobank_ids.append(participant.biobankId)
            self.assertEqual(summary_dao.get(participant.participantId).numBaselineSamplesArrived, 0)

        test_codes = random.sample(_BASELINE_TESTS, nids)
        samples_file = test_data.open_biobank_samples(biobank_ids=biobank_ids, tests=test_codes)
        lines = samples_file.split("\n")[1:]  # remove field name line

        input_filename = "cloud%s.csv" % self._naive_utc_to_naive_central(clock.CLOCK.now()).strftime(
            biobank_samples_pipeline.INPUT_CSV_TIME_FORMAT
        )
        self._write_cloud_csv(input_filename, samples_file)
        biobank_samples_pipeline.upsert_from_latest_csv()

        self.assertEqual(dao.count(), nids - cids)

        for x in range(0, nids):
            cols = lines[x].split("\t")

            if cols[10].strip():  # skip child sample
                continue

            # If status is 'In Prep', then sample confirmed timestamp should be empty
            if cols[2] == "In Prep":
                self.assertEqual(len(cols[11]), 0)
            else:
                status = SampleStatus.RECEIVED
                ts_str = cols[11]
                # DA-814 - Participant Summary test status should be: Unset, Received or Disposed only.
                # If sample is disposed, then check disposed timestamp, otherwise check confirmed timestamp.
                # DA-871 - Only check status is disposed when reason code is a bad disposal.
                if cols[2] == "Disposed" and get_sample_status_enum_value(cols[8]) > SampleStatus.UNKNOWN:
                    status = SampleStatus.DISPOSED
                    ts_str = cols[9]

                ts = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S")
                self._check_summary(participant_ids[x], test_codes[x], ts, status)

    def test_old_csv_not_imported(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        now = clock.CLOCK.now()
        too_old_time = now - timedelta(hours=25)
        input_filename = "cloud%s.csv" % self._naive_utc_to_naive_central(too_old_time).strftime(
            biobank_samples_pipeline.INPUT_CSV_TIME_FORMAT
        )
        self._write_cloud_csv(input_filename, "")
        with self.assertRaises(biobank_samples_pipeline.DataError):
            biobank_samples_pipeline.upsert_from_latest_csv()

    def _naive_utc_to_naive_central(self, naive_utc_date):
        utc_date = pytz.utc.localize(naive_utc_date)
        central_date = utc_date.astimezone(pytz.timezone("US/Central"))
        return central_date.replace(tzinfo=None)

    def _check_summary(self, participant_id, test, date_formatted, status):
        summary = ParticipantSummaryDao().get(participant_id)
        self.assertEqual(summary.numBaselineSamplesArrived, 1)
        # DA-614 - All specific disposal statuses in biobank_stored_samples are changed to DISPOSED
        # in the participant summary.
        self.assertEqual(status, getattr(summary, "sampleStatus" + test))
        sample_time = self._naive_utc_to_naive_central(getattr(summary, "sampleStatus" + test + "Time"))
        self.assertEqual(date_formatted, sample_time)

    def test_find_latest_csv(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        # The cloud storage testbed does not expose an injectable time function.
        # Creation time is stored at second granularity.
        self._write_cloud_csv("a_lex_first_created_first.csv", "any contents")
        time.sleep(1.0)
        self._write_cloud_csv("z_lex_last_created_middle.csv", "any contents")
        time.sleep(1.0)
        created_last = "b_lex_middle_created_last.csv"
        self._write_cloud_csv(created_last, "any contents")
        self._write_cloud_csv(
            "%s/created_last_in_subdir.csv" % biobank_samples_pipeline._REPORT_SUBDIR, "any contents"
        )

        latest_filename = biobank_samples_pipeline._find_latest_samples_csv(_FAKE_BUCKET)
        self.assertEqual(latest_filename, "%s" % created_last)

    def test_sample_from_row(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        samples_file = test_data.open_biobank_samples([112, 222, 333], [])
        reader = csv.DictReader(io.StringIO(samples_file), delimiter="\t")
        row = next(reader)
        sample = biobank_samples_pipeline._create_sample_from_row(row, get_biobank_id_prefix())
        self.assertIsNotNone(sample)

        cols = biobank_samples_pipeline.CsvColumns
        self.assertEqual(sample.biobankStoredSampleId, row[cols.SAMPLE_ID])
        self.assertEqual(to_client_biobank_id(sample.biobankId), row[cols.EXTERNAL_PARTICIPANT_ID])
        self.assertEqual(sample.test, row[cols.TEST_CODE])
        confirmed_date = self._naive_utc_to_naive_central(sample.confirmed)
        self.assertEqual(
            confirmed_date.strftime(biobank_samples_pipeline._INPUT_TIMESTAMP_FORMAT), row[cols.CONFIRMED_DATE]
        )
        received_date = self._naive_utc_to_naive_central(sample.created)
        self.assertEqual(
            received_date.strftime(biobank_samples_pipeline._INPUT_TIMESTAMP_FORMAT), row[cols.CREATE_DATE]
        )

    def test_sample_from_row_wrong_prefix(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        samples_file = test_data.open_biobank_samples([111, 222, 333], [])
        reader = csv.DictReader(io.StringIO(samples_file), delimiter="\t")
        row = next(reader)
        row[biobank_samples_pipeline.CsvColumns.CONFIRMED_DATE] = "2016 11 19"
        self.assertIsNone(biobank_samples_pipeline._create_sample_from_row(row, "Q"))

    def test_sample_from_row_invalid(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        samples_file = test_data.open_biobank_samples([111, 222, 333], [])
        reader = csv.DictReader(io.StringIO(samples_file), delimiter="\t")
        row = next(reader)
        row[biobank_samples_pipeline.CsvColumns.CONFIRMED_DATE] = "2016 11 19"
        with self.assertRaises(biobank_samples_pipeline.DataError):
            biobank_samples_pipeline._create_sample_from_row(row, get_biobank_id_prefix())

    def test_sample_from_row_old_test(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        samples_file = test_data.open_biobank_samples([111, 222, 333], [])
        reader = csv.DictReader(io.StringIO(samples_file), delimiter="\t")
        row = next(reader)
        row[biobank_samples_pipeline.CsvColumns.TEST_CODE] = "2PST8"
        sample = biobank_samples_pipeline._create_sample_from_row(row, get_biobank_id_prefix())
        self.assertIsNotNone(sample)
        cols = biobank_samples_pipeline.CsvColumns
        self.assertEqual(sample.biobankStoredSampleId, row[cols.SAMPLE_ID])
        self.assertEqual(sample.test, row[cols.TEST_CODE])

    def test_column_missing(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        with open(test_data.data_path("biobank_samples_missing_field.csv")) as samples_file:
            reader = csv.DictReader(samples_file, delimiter="\t")
            with self.assertRaises(biobank_samples_pipeline.DataError):
                biobank_samples_pipeline._upsert_samples_from_csv(reader)

    def test_wrong_csv_delimiter(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        # Use a valid file containing commas as separators
        with open(test_data.data_path("biobank_samples_wrong_delimiter.csv")) as samples_file:
            reader = csv.DictReader(samples_file, delimiter="\t")
            with self.assertRaises(biobank_samples_pipeline.DataError):
                biobank_samples_pipeline._upsert_samples_from_csv(reader)

    def test_get_reconciliation_report_paths(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        dt = datetime(2016, 12, 22, 18, 30, 45)
        expected_prefix = "reconciliation/report_2016-12-22"
        paths = biobank_samples_pipeline._get_report_paths(dt)
        self.assertEqual(len(paths), 5)
        for path in paths:
            self.assertTrue(
                path.startswith(expected_prefix), "Report path %r must start with %r." % (expected_prefix, path)
            )
            self.assertTrue(path.endswith(".csv"))

    def _init_report_codes(self):
        self.create_database_code(system=PPI_SYSTEM, value=RACE_QUESTION_CODE)
        self.create_database_code(system=PPI_SYSTEM, value=RACE_AIAN_CODE)

    @staticmethod
    def _datetime_days_ago(num_days_ago):
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        return today - timedelta(days=num_days_ago)

    @staticmethod
    def _format_datetime(timestamp):
        return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

    def test_quest_samples_in_report(self):
        self._init_report_codes()

        # Generate data for a Quest sample to be in the report
        participant = self.create_database_participant()
        order = self.create_database_biobank_order(participantId=participant.participantId)
        order_identifier = self.create_database_biobank_order_identifier(
            biobankOrderId=order.biobankOrderId,
            value='KIT-001',
            system=_MAYO_KIT_SYSTEM
        )
        ordered_sample = self.create_database_biobank_ordered_sample(
            biobankOrderId=order.biobankOrderId,
            collected=datetime(2020, 6, 5),
            processed=datetime(2020, 6, 6),
            finalized=datetime(2020, 6, 7)
        )
        stored_sample = self.create_database_biobank_stored_sample(
            test=ordered_sample.test,
            biobankId=participant.biobankId,
            biobankOrderIdentifier=order_identifier.value,
            confirmed=self._datetime_days_ago(7)
        )

        # Mocking the file writer to catch what gets exported and mocking the upload method because
        # that isn't what this test is meant to cover
        with mock.patch('rdr_service.offline.sql_exporter.csv.writer') as mock_writer_class,\
                mock.patch('rdr_service.offline.sql_exporter.SqlExporter.upload_export_file'):
            biobank_samples_pipeline.write_reconciliation_report(datetime.now())

            mock_write_rows = mock_writer_class.return_value.writerows
            mock_write_rows.assert_called_once_with([(
                f'Z{participant.biobankId}',
                ordered_sample.test,
                Decimal('1'),  # sent count
                order_identifier.value,
                self._format_datetime(ordered_sample.collected),
                self._format_datetime(ordered_sample.processed),
                self._format_datetime(ordered_sample.finalized),
                None, None, None, 'UNKNOWN',  # site info: name, client_number, hpo, hpo_type
                None, None, None, 'UNKNOWN',  # finalized site info: name, client_number, hpo, hpo_type
                None,  # finalized username
                ordered_sample.test,
                1,  # received count
                str(stored_sample.biobankStoredSampleId),
                self._format_datetime(stored_sample.confirmed),  # received time
                None,  # created family date
                None,  # elapsed hours
                order_identifier.value,  # kit identifier
                None,  # fedex tracking number
                'N',  # is Native American
                None, None, None,  # notes info: collected, processed, finalized
                None, None, None, None, None,  # cancelled_restored info: status_flag, name, name, time, reason
                None  # order origin
            )])


