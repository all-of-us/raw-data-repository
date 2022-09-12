from collections import namedtuple
import csv
from decimal import Decimal
import io
from datetime import datetime, timedelta
import mock
import random
import os
import pytz

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file
from rdr_service.code_constants import BIOBANK_TESTS
from rdr_service.config import BIOBANK_SAMPLES_DAILY_INVENTORY_FILE_PATTERN,\
    BIOBANK_SAMPLES_MONTHLY_INVENTORY_FILE_PATTERN
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.config_utils import get_biobank_id_prefix, to_client_biobank_id
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.offline import biobank_samples_pipeline
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.participant_enums import EnrollmentStatus, SampleStatus, get_sample_status_enum_value,\
    SampleCollectionMethod
from rdr_service.services.system_utils import DateRange
from tests import test_data
from tests.helpers.unittest_base import BaseTestCase, PDRGeneratorTestMixin

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = "rdr_fake_bucket"

FakeBlob = namedtuple('FakeBlob', ['name', 'updated'])


class BiobankSamplesPipelineTest(BaseTestCase, PDRGeneratorTestMixin):
    def setUp(self):
        super(BiobankSamplesPipelineTest, self).setUp()
        config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, _BASELINE_TESTS)
        # Everything is stored as a list, so override bucket name as a 1-element list.
        config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BUCKET])
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()

        config.override_setting(BIOBANK_SAMPLES_DAILY_INVENTORY_FILE_PATTERN, 'Sample Inventory Report v1')
        config.override_setting(BIOBANK_SAMPLES_MONTHLY_INVENTORY_FILE_PATTERN, 'Sample Inventory Report 60d')

        self.data_generator.initialize_common_codes()

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
            ("mailKitOrders", [BiobankMailKitOrder(participantId=participantId, modified=modified, version=1)]),
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

    def test_core_participants_stay_core(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        participant = self.participant_dao.insert(Participant())
        self.summary_dao.insert(self.participant_summary(participant))

        with self.summary_dao.session() as session:
            participant_summary = session.query(ParticipantSummary).filter(
                ParticipantSummary.participantId == participant.participantId
            ).one()
            participant_summary.enrollmentStatus = EnrollmentStatus.FULL_PARTICIPANT

        # This updates all participants, regardless of whether they have samples imported or not
        self.summary_dao.update_from_biobank_stored_samples()

        ps = self.summary_dao.get(participant.participantId)
        self.assertEqual(EnrollmentStatus.FULL_PARTICIPANT, ps.enrollmentStatus)

    @mock.patch('rdr_service.dao.participant_summary_dao.QuestionnaireResponseRepository')
    @mock.patch('rdr_service.offline.biobank_samples_pipeline.dispatch_participant_rebuild_tasks')
    def test_end_to_end(self, mock_dispatch_rebuild, response_repository):
        response_repository.get_interest_in_sharing_ehr_ranges.return_value = [
            DateRange(start=datetime(2016, 11, 29, 12, 16))
        ]

        config.override_setting(BIOBANK_SAMPLES_DAILY_INVENTORY_FILE_PATTERN, 'cloud')

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

        # Arbitrarily pick samples to be used for testing 1SAL2 collection method checking
        mail_kit_1sal2_participant_id = participant_ids[6]
        on_site_1sal2_participant_id = participant_ids[11]
        no_order_1sal2_participant_id = participant_ids[14]
        core_minus_pm_participant_id = participant_ids[5]
        core_participant_id = participant_ids[1]
        test_codes[6] = test_codes[11] = test_codes[14] = test_codes[5] = test_codes[1] = '1SAL2'

        mailed_biobank_order = self.data_generator.create_database_biobank_order(
            participantId=mail_kit_1sal2_participant_id
        )
        self.data_generator.create_database_biobank_order_identifier(
            biobankOrderId=mailed_biobank_order.biobankOrderId,
            value='KIT-6'  # from the 7th record in biobank_samples_1.csv
        )
        self.data_generator.create_database_biobank_mail_kit_order(
            biobankOrderId=mailed_biobank_order.biobankOrderId,
            participantId=mail_kit_1sal2_participant_id
        )

        on_site_biobank_order = self.data_generator.create_database_biobank_order(
            participantId=on_site_1sal2_participant_id
        )
        self.data_generator.create_database_biobank_order_identifier(
            biobankOrderId=on_site_biobank_order.biobankOrderId,
            value='KIT-11'  # from the 12th record in the biobank_samples_1.csv
        )
        participant_summary_dao = ParticipantSummaryDao()
        core_minus_pm_summary = participant_summary_dao.get(core_minus_pm_participant_id)

        core_minus_pm_summary.numCompletedBaselinePPIModules = 3
        core_minus_pm_summary.consentForStudyEnrollment = 1
        core_minus_pm_summary.consentForElectronicHealthRecords = 1
        core_minus_pm_summary.enrollmentStatusMemberTime = '2016-11-29 12:16:00'
        core_minus_pm_summary.questionnaireOnTheBasicsAuthored = '2016-11-29 12:16:00'
        core_minus_pm_summary.questionnaireOnOverallHealthAuthored = '2016-11-29 12:16:00'
        core_minus_pm_summary.questionnaireOnLifestyleAuthored = '2016-11-29 12:16:00'
        participant_summary_dao.update(core_minus_pm_summary)

        core_summary = participant_summary_dao.get(core_participant_id)

        core_summary.numCompletedBaselinePPIModules = 3
        core_summary.consentForStudyEnrollment = 1
        core_summary.consentForElectronicHealthRecords = 1
        core_summary.enrollmentStatusMemberTime = '2016-11-29 12:16:00'
        core_summary.questionnaireOnTheBasicsAuthored = '2016-11-29 12:16:00'
        core_summary.questionnaireOnOverallHealthAuthored = '2016-11-29 12:16:00'
        core_summary.questionnaireOnLifestyleAuthored = '2016-11-29 12:16:00'
        core_summary.clinicPhysicalMeasurementsStatus = 1
        core_summary.clinicPhysicalMeasurementsFinalizedTime = '2016-11-29 12:16:00'
        participant_summary_dao.update(core_summary)

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

        # Check that the 1SAL2 collection methods were set correctly
        on_site_summary: ParticipantSummary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == on_site_1sal2_participant_id
        ).one()
        self.assertEqual(SampleCollectionMethod.ON_SITE, on_site_summary.sample1SAL2CollectionMethod)

        mail_kit_summary: ParticipantSummary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == mail_kit_1sal2_participant_id
        ).one()
        self.assertEqual(SampleCollectionMethod.MAIL_KIT, mail_kit_summary.sample1SAL2CollectionMethod)

        core_minus_pm_summary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == core_minus_pm_participant_id
        ).one()
        self.assertEqual(core_minus_pm_summary.enrollmentStatus, EnrollmentStatus.CORE_MINUS_PM)
        self.assertEqual(core_minus_pm_summary.enrollmentStatusCoreMinusPMTime, datetime(2017, 11, 30, 2, 59, 42))

        core_summary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == core_participant_id
        ).one()
        self.assertEqual(core_summary.enrollmentStatus, EnrollmentStatus.FULL_PARTICIPANT)
        self.assertEqual(core_summary.enrollmentStatusCoreStoredSampleTime, datetime(2016, 11, 29, 18, 38, 58))

        no_order_summary: ParticipantSummary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == no_order_1sal2_participant_id
        ).one()
        self.assertIsNone(no_order_summary.sample1SAL2CollectionMethod)

        # Check for bigquery_sync record updates.  Only expect updates for the pids with orders
        rebuilt_participant_list = mock_dispatch_rebuild.call_args[0][0]
        self.assertIn(on_site_1sal2_participant_id, rebuilt_participant_list)
        self.assertIn(mail_kit_1sal2_participant_id, rebuilt_participant_list)
        self.assertNotIn(no_order_1sal2_participant_id, rebuilt_participant_list)

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

    @mock.patch('rdr_service.offline.biobank_samples_pipeline.list_blobs')
    def test_find_latest_csv(self, mock_list_blobs):
        mock_list_blobs.return_value = [
            FakeBlob(name='Sample Inventory Report v12020-06-01.csv',  # older file
                     updated=datetime(2020, 6, 1)),
            FakeBlob(name='Sample Inventory Report v12020-07-14.csv',  # last inventory file (should use this one)
                     updated=datetime(2020, 7, 14)),
            FakeBlob(name='not an inventory file v12020-08-01.csv',  # not the correct name pattern
                     updated=datetime(2020, 8, 1)),
            FakeBlob(name='60_day_manifests/Sample Inventory Report 60d2020-08-02-04-00-21.csv',  # 60 day manifest
                     updated=datetime(2020, 8, 2)),
            FakeBlob(name='genomic_samples_manifests/Genomic-Manifest-AoU-4-2020-06-30-08-22-52_C2.CSV',  # genomic file
                     updated=datetime(2020, 8, 3)),
            FakeBlob(name='Sample Inventory Report v12020-08-04',  # not a csv
                     updated=datetime(2020, 8, 4)),
        ]  # todo: make sure file names that get listed by google actually look like this

        latest_filename = biobank_samples_pipeline._find_latest_samples_csv(_FAKE_BUCKET)
        self.assertEqual('Sample Inventory Report v12020-07-14.csv', latest_filename)

    @mock.patch('rdr_service.offline.biobank_samples_pipeline.list_blobs')
    def test_find_latest_csv(self, mock_list_blobs):
        mock_list_blobs.return_value = [
            FakeBlob(name='60_day_manifests/Sample Inventory Report 60d2020-06-01-04-00-21.csv',  # older file
                     updated=datetime(2020, 6, 1)),
            FakeBlob(name='60_day_manifests/Sample Inventory Report 60d2020-07-14-04-00-21.csv',  # current manifest
                     updated=datetime(2020, 7, 14)),
            FakeBlob(name='not an inventory file 60d2020-08-01.csv',  # not the correct name pattern
                     updated=datetime(2020, 8, 1)),
            FakeBlob(name='Sample Inventory Report v12020-08-02.csv',  # single day inventory file
                     updated=datetime(2020, 8, 2)),
            FakeBlob(name='genomic_samples_manifests/Genomic-Manifest-AoU-4-2020-06-30-08-22-52_C2.CSV',  # genomic file
                     updated=datetime(2020, 8, 3)),
            FakeBlob(name='60_day_manifests/Sample Inventory Report 60d2020-08-04',  # not a csv
                     updated=datetime(2020, 8, 4)),
        ]  # todo: make sure file names that get listed by google actually look like this

        latest_filename = biobank_samples_pipeline._find_latest_samples_csv(_FAKE_BUCKET, monthly=True)
        self.assertEqual('60_day_manifests/Sample Inventory Report 60d2020-07-14-04-00-21.csv', latest_filename)

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
        self.assertEqual(len(paths), 4)
        for path in paths:
            self.assertTrue(
                path.startswith(expected_prefix), "Report path %r must start with %r." % (expected_prefix, path)
            )
            self.assertTrue(path.endswith(".csv"))

    @staticmethod
    def _datetime_days_ago(num_days_ago):
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        return today - timedelta(days=num_days_ago)

    @staticmethod
    def _format_datetime(timestamp):
        return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

    def test_quest_samples_in_report(self):
        # Generate data for a Quest sample to be in the report
        participant = self.data_generator.create_database_participant()
        order = self.data_generator.create_database_biobank_order(participantId=participant.participantId)

        # Setup order identifiers that CE would send for Quest order
        self.data_generator.create_database_biobank_order_identifier(
            biobankOrderId=order.biobankOrderId,
            value='aoeu-1234',  # CE CareTask system doesn't use the KIT numbers for identifiers
            system=biobank_samples_pipeline._CE_QUEST_SYSTEM
        )
        kit_order_identifier = self.data_generator.create_database_biobank_order_identifier(
            biobankOrderId=order.biobankOrderId,
            value='KIT-001',
            system=biobank_samples_pipeline._KIT_ID_SYSTEM
        )

        ordered_sample = self.data_generator.create_database_biobank_ordered_sample(
            biobankOrderId=order.biobankOrderId,
            collected=datetime(2020, 6, 5),
            processed=datetime(2020, 6, 6),
            finalized=datetime(2020, 6, 7)
        )
        stored_sample = self.data_generator.create_database_biobank_stored_sample(
            test=ordered_sample.test,
            biobankId=participant.biobankId,
            biobankOrderIdentifier=kit_order_identifier.value,
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
                kit_order_identifier.value,
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
                'KIT-001',  # kit identifier
                None,  # fedex tracking number
                'N',  # is Native American
                None, None, None,  # notes info: collected, processed, finalized
                None, None, None, None, None,  # cancelled_restored info: status_flag, name, name, time, reason
                None,  # order origin
                'example'  # Participant origin
            )])

    def test_demographic_flags_in_received_report(self):
        self.temporarily_override_config_setting(config.ENABLE_BIOBANK_MANIFEST_RECEIVED_FLAG, 1)

        # Generate data for a New York sample to be in the report
        participant = self.data_generator.create_database_participant()
        collection_site = self.data_generator.create_database_site(state='NY')
        order = self.data_generator.create_database_biobank_order(
            participantId=participant.participantId,
            collectedSiteId=collection_site.siteId
        )

        # Setup order identifiers that CE would send for Quest order
        self.data_generator.create_database_biobank_order_identifier(
            biobankOrderId=order.biobankOrderId,
            value='aoeu-1234',  # CE CareTask system doesn't use the KIT numbers for identifiers
            system=biobank_samples_pipeline._CE_QUEST_SYSTEM
        )
        kit_order_identifier = self.data_generator.create_database_biobank_order_identifier(
            biobankOrderId=order.biobankOrderId,
            value='KIT-001',
            system=biobank_samples_pipeline._KIT_ID_SYSTEM
        )

        ordered_sample = self.data_generator.create_database_biobank_ordered_sample(
            biobankOrderId=order.biobankOrderId,
            collected=datetime(2020, 6, 5),
            processed=datetime(2020, 6, 6),
            finalized=datetime(2020, 6, 7)
        )
        stored_sample = self.data_generator.create_database_biobank_stored_sample(
            test=ordered_sample.test,
            biobankId=participant.biobankId,
            biobankOrderIdentifier=kit_order_identifier.value,
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
                kit_order_identifier.value,
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
                'KIT-001',  # kit identifier
                None,  # fedex tracking number
                'N',  # is Native American
                None, None, None,  # notes info: collected, processed, finalized
                None, None, None, None, None,  # cancelled_restored info: status_flag, name, name, time, reason
                None,  # order origin
                'example',  # Participant origin,
                'Y',  # NY flag
                'NA'  # sex at birth flag
            )])

    def _generate_withdrawal_report(self):
        with mock.patch('rdr_service.offline.sql_exporter.csv.writer') as mock_writer_class,\
                mock.patch('rdr_service.offline.sql_exporter.SqlExporter.upload_export_file'):

            # Generate the withdrawal report
            day_range_of_report = 10
            biobank_samples_pipeline._query_and_write_withdrawal_report(
                SqlExporter(''), '', day_range_of_report, datetime.now()
            )

            # Check the header values written
            mock_writer_class.return_value.writerow.assert_called_with(
                ['biobank_id', 'withdrawal_time', 'is_native_american', 'needs_disposal_ceremony', 'participant_origin']
            )

            mock_write_rows = mock_writer_class.return_value.writerows
            return mock_write_rows.call_args[0][0] if mock_write_rows.called else []

    def test_cumulative_received_report(self):
        current_datetime = datetime.now()
        cumulative_report_datetime = current_datetime + timedelta(days=5)
        self.temporarily_override_config_setting(
            config.BIOBANK_CUMULATIVE_RECEIVED_SCHEDULE,
            {
                '2020-10-30': '2020-07-01',
                cumulative_report_datetime.strftime('%Y-%m-%d'): '2021-08-01'
            }
        )

        # Make sure the cumulative received report isn't generated when the date isn't in the config
        exporter_mock = mock.MagicMock()
        biobank_samples_pipeline._query_and_write_reports(
            exporter=exporter_mock,
            now=current_datetime,
            report_type='daily',
            path_received='received',
            path_missing='missing',
            path_modified='modified'
        )
        for call in exporter_mock.run_export.call_args_list:
            path_name, *_ = call.args
            self.assertNotIn('cumulative_received', path_name)

        # Make sure the cumulative report gets generated on the right day and with the right start date
        exporter_mock = mock.MagicMock()
        biobank_samples_pipeline._query_and_write_reports(
            exporter=exporter_mock,
            now=cumulative_report_datetime,
            report_type='daily',
            path_received='received',
            path_missing='missing',
            path_modified='modified'
        )
        cumulative_report_params = None
        for call in exporter_mock.run_export.call_args_list:
            if len(call.args) > 2:
                path_name = call.args[0]
                if 'cumulative_received' in path_name:
                    cumulative_report_params = call.args[2]

        self.assertIsNotNone(cumulative_report_params)
        self.assertEqual(datetime(2021, 8, 1), cumulative_report_params['n_days_ago'])
