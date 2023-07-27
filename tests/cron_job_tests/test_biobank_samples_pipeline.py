from collections import namedtuple
from decimal import Decimal
from datetime import datetime, timedelta
import mock
import random
import os

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file, get_blob
from rdr_service.code_constants import BIOBANK_TESTS
from rdr_service.config import BIOBANK_SAMPLES_DAILY_INVENTORY_FILE_PATTERN,\
    BIOBANK_SAMPLES_MONTHLY_INVENTORY_FILE_PATTERN
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.biobank_order import (
    BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample, BiobankOrderStatus
)
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.config_utils import from_client_biobank_id
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

    mock_bucket_paths = [_FAKE_BUCKET, _FAKE_BUCKET + os.sep + biobank_samples_pipeline._REPORT_SUBDIR,
                         _FAKE_BUCKET + os.sep + biobank_samples_pipeline._OVERDUE_DNA_SAMPLES_SUBDIR]

    # Set up values used by the overdue samples report test cases
    overdue_samples_report_dt = datetime.utcnow().replace(microsecond=0)
    overdue_samples_blobname = biobank_samples_pipeline._get_report_path(
        overdue_samples_report_dt, 'overdue_dna_samples',
        report_subdir=biobank_samples_pipeline._OVERDUE_DNA_SAMPLES_SUBDIR
    )

    def _verify_overdue_samples_report(self, biobank_id=None, biobank_order=None,
                                       finalized=None, overdue_samples=None):
        blob = get_blob(_FAKE_BUCKET, blob_name=self.overdue_samples_blobname)
        self.assertIsNotNone(blob)
        with open_cloud_file("/%s/%s" % (_FAKE_BUCKET, self.overdue_samples_blobname)) as cloud_file:
            lines = cloud_file.readlines()
            if not overdue_samples:
                # No overdue samples expected in this report; verify it's a header row only
                self.assertEqual(1, len(lines))
                self.assertEqual(lines[0].rstrip(),
                                 'biobank_id,biobank_order_id,order_finalized_date,overdue_dna_samples')
            else:
                # Verify the data row expected by the test case
                self.assertEqual(lines[1].rstrip(),
                                 f'{biobank_id},{biobank_order},{finalized},{overdue_samples}')
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
    def test_end_to_end(self, response_repository):
        response_repository.get_interest_in_sharing_ehr_ranges.return_value = [
            DateRange(start=datetime(2016, 11, 29, 12, 16))
        ]

        dao = BiobankStoredSampleDao()
        # Create 3 participants and pass their (random) IDs into sample rows.
        summary_dao = ParticipantSummaryDao()
        biobank_ids = []
        participant_ids = []
        nids = 14  # equal to the number of parent rows in 'biobank_samples_1.csv'
        cids = 1  # equal to the number of child rows in 'biobank_samples_1.csv'
        biobank_pid_map = {}

        for _ in range(nids):
            participant = self.participant_dao.insert(Participant())
            summary_dao.insert(self.participant_summary(participant))
            participant_ids.append(participant.participantId)
            biobank_ids.append(participant.biobankId)
            biobank_pid_map[participant.biobankId] = participant.participantId
            self.assertEqual(summary_dao.get(participant.participantId).numBaselineSamplesArrived, 0)

        test_codes = random.sample(_BASELINE_TESTS, nids)

        # Arbitrarily pick samples to be used for testing 1SAL2 collection method checking
        mail_kit_1sal2_participant_id = participant_ids[6]
        on_site_1sal2_participant_id = participant_ids[11]
        no_order_1sal2_participant_id = participant_ids[13]
        core_minus_pm_participant_id = participant_ids[5]
        core_participant_id = participant_ids[1]
        test_codes[6] = test_codes[11] = test_codes[13] = test_codes[5] = test_codes[1] = '1SAL2'

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

        samples_json = test_data.open_biobank_samples(biobank_ids=biobank_ids, tests=test_codes)
        self.send_put('Biobank/specimens', request_data=samples_json)

        self.assertEqual(dao.count(), nids - cids)

        for sample_json in samples_json:
            status = SampleStatus.RECEIVED
            status_date_str = sample_json['confirmationDate']
            # DA-814 - Participant Summary test status should be: Unset, Received or Disposed only.
            # If sample is disposed, then check disposed timestamp, otherwise check confirmed timestamp.
            # DA-871 - Only check status is disposed when reason code is a bad disposal.
            if sample_json['status']['status'] == "Disposed" \
                    and get_sample_status_enum_value(sample_json['disposalStatus']['reason']) > SampleStatus.UNKNOWN:
                status = SampleStatus.DISPOSED
                status_date_str = sample_json['disposalStatus']['disposalDate']

            ts = datetime.strptime(status_date_str, "%Y/%m/%d %H:%M:%S")
            self._check_summary(
                biobank_pid_map[from_client_biobank_id(sample_json['participantID'])],
                sample_json['testcode'],
                ts,
                status
            )

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
        self.assertEqual(core_minus_pm_summary.enrollmentStatusCoreMinusPMTime, datetime(2017, 11, 29, 16, 10, 17))

        core_summary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == core_participant_id
        ).one()
        self.assertEqual(core_summary.enrollmentStatus, EnrollmentStatus.FULL_PARTICIPANT)
        self.assertEqual(core_summary.enrollmentStatusCoreStoredSampleTime, datetime(2016, 11, 29, 12, 38, 58))

        no_order_summary: ParticipantSummary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == no_order_1sal2_participant_id
        ).one()
        self.assertIsNone(no_order_summary.sample1SAL2CollectionMethod)

    def _check_summary(self, participant_id, test, date_formatted, status):
        summary = ParticipantSummaryDao().get(participant_id)
        self.assertEqual(summary.numBaselineSamplesArrived, 1)
        # DA-614 - All specific disposal statuses in biobank_stored_samples are changed to DISPOSED
        # in the participant summary.
        self.assertEqual(status, getattr(summary, "sampleStatus" + test))
        sample_time = getattr(summary, "sampleStatus" + test + "Time")
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

    def test_overdue_dna_sample_report(self):
        self.temporarily_override_config_setting(config.RDR_SLACK_WEBHOOKS, {
            'rdr_biobank_missing_samples_webhook': 'fakewh12345'
        })
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)

        participant = self.data_generator.create_database_participant()
        # Biobank order/samples finalized 7 or more days ago (since today's date)
        today = datetime.utcnow().replace(microsecond=0)
        order_ts = today - timedelta(days=8)
        order = self.data_generator.create_database_biobank_order(
            participantId=participant.participantId,
            orderOrigin='hpro',
            orderStatus=BiobankOrderStatus.UNSET,
            finalizedTime=order_ts
        )
        order_identifier = self.data_generator.create_database_biobank_order_identifier(
            value='1234ABC',
            biobankOrderId=order.biobankOrderId
        )
        # Create DNA and non-DNA ordered samples, but only non-DNA test has a stored sample record
        dna_ordered_sample = self.data_generator.create_database_biobank_ordered_sample(
            biobankOrderId=order.biobankOrderId,
            test='1ED04',
            collected=order_ts,
            processed=order_ts,
            finalized=order_ts
        )
        non_dna_ordered_sample = self.data_generator.create_database_biobank_ordered_sample(
            biobankOrderId=order.biobankOrderId,
            test='1UR10',
            collected=order_ts,
            processed=order_ts,
            finalized=order_ts
        )
        self.data_generator.create_database_biobank_stored_sample(
            biobankId=participant.biobankId,
            biobankOrderIdentifier=order_identifier.value,
            test=non_dna_ordered_sample.test,
            status=SampleStatus.RECEIVED
        )

        # Expect Slack notification that the DNA sample is overdue
        biobank_prefix = config.getSetting(config.BIOBANK_ID_PREFIX)
        expected_msg_substrings = [
            f'Biobank ID: {biobank_prefix}{participant.biobankId}, order: {order.biobankOrderId}, ',
            f'finalized: {dna_ordered_sample.finalized}, overdue ordered DNA samples: {dna_ordered_sample.test}'
        ]

        with mock.patch('rdr_service.offline.biobank_samples_pipeline.logging') as mock_logging, \
             mock.patch('rdr_service.services.slack_utils.SlackMessageHandler.send_message_to_webhook') as mock_slack:
            biobank_samples_pipeline.overdue_samples_check(self.overdue_samples_report_dt)
            # Verify generated bucket file
            blob = get_blob(_FAKE_BUCKET, blob_name=self.overdue_samples_blobname)
            self.assertIsNotNone(blob)
            self._verify_overdue_samples_report(biobank_id=f'{biobank_prefix}{participant.biobankId}',
                                                biobank_order=order.biobankOrderId,
                                                finalized=order_ts.strftime("%Y-%m-%d %H:%M:%S"),
                                                overdue_samples='1ED04')
            # Verify Slack message populated from bucket file rows
            error_log_call = mock_logging.error.call_args
            slack_webhook_args = mock_slack.call_args
            self.assertIsNotNone(error_log_call, 'An error log call should have been made')
            self.assertIsNotNone(slack_webhook_args, 'A slack notification should have been made')
            for msg_str in expected_msg_substrings:
                self.assertIn(msg_str, error_log_call.args[0])
                self.assertIn(msg_str, slack_webhook_args.kwargs['message_data']['text'])

    def test_cancelled_order_sample_not_overdue(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        participant = self.data_generator.create_database_participant()
        today = datetime.utcnow().replace(microsecond=0)
        order_ts = today - timedelta(days=9)
        # If order is cancelled, missing samples check should not generate any notification
        order = self.data_generator.create_database_biobank_order(
            participantId=participant.participantId,
            orderOrigin='hpro',
            orderStatus=BiobankOrderStatus.CANCELLED,
            finalizedTime=order_ts
        )
        self.data_generator.create_database_biobank_order_identifier(
            value='1234ABC',
            biobankOrderId=order.biobankOrderId
        )
        self.data_generator.create_database_biobank_ordered_sample(
            biobankOrderId=order.biobankOrderId,
            test='1ED04',
            collected=order_ts,
            processed=order_ts,
            finalized=order_ts
        )

        with mock.patch('rdr_service.offline.biobank_samples_pipeline.logging') as mock_logging:
            biobank_samples_pipeline.overdue_samples_check(self.overdue_samples_report_dt)
            # Verify there was an empty file (header row only) dropped to the bucket
            self._verify_overdue_samples_report(overdue_samples=None)
            # Verify there were no errors logged (logging also triggers slack notification)
            error_log_call = mock_logging.error.call_args
            self.assertIsNone(error_log_call, "Missing sample notification not expected for cancelled orders")

    def test_sample_not_yet_overdue(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        participant = self.data_generator.create_database_participant()
        order_ts = self.overdue_samples_report_dt - timedelta(days=6)
        # Should not be notified of overdue samples if less than 7 days since order was finalized
        order = self.data_generator.create_database_biobank_order(
            participantId=participant.participantId,
            orderOrigin='hpro',
            orderStatus=BiobankOrderStatus.UNSET,
            finalizedTime=order_ts
        )
        self.data_generator.create_database_biobank_order_identifier(
            value='1234ABC',
            biobankOrderId=order.biobankOrderId
        )
        self.data_generator.create_database_biobank_ordered_sample(
            biobankOrderId=order.biobankOrderId,
            test='1ED04',
            collected=order_ts,
            processed=order_ts,
            finalized=order_ts
        )

        with mock.patch('rdr_service.offline.biobank_samples_pipeline.logging') as mock_logging:
            biobank_samples_pipeline.overdue_samples_check(self.overdue_samples_report_dt)
            # Verify there was an empty file (header row only) dropped to the bucket
            self._verify_overdue_samples_report(overdue_samples=None)
            # Verify there were no errors logged (logging also triggers slack notification)
            error_log_call = mock_logging.error.call_args
            self.assertIsNone(error_log_call, "Overdue sample notification not expected for orders under a week old")
