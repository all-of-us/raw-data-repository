import datetime
from unittest import mock

from rdr_service import config
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.ppsc_partner_transfer_dao import PPSCDataTransferBaseDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.ppsc_partner_data_transfer import PPSCEHR
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.workflow_management.ppsc.ppsc_to_legacy_de_mappings import map_source_to_summary, consent_data_elements
from tests.service_tests.test_genomic_datagen import GenomicDataGenMixin
from tests.workflow_tests.test_data.ppsc_data_feed_test_data import core_data_expected_sql, biospecimen_expected_sql, \
    ehr_expected_sql, health_data_sharing_expected_sql, ehr_expected_streaming_sql, core_data_expected_streaming_sql, \
    biospecimen_expected_streaming_sql, health_data_expected_streaming_sql, consent_activity_expected_sql
from rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed import InputFeed, Intake2SummaryFeed


class DataTransferInputTest(GenomicDataGenMixin):
    def setUp(self, *args, **kwargs) -> None:
        # pylint: disable=unused-argument
        self.temporarily_override_config_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, ["participant_ehr"])
        self.temporarily_override_config_setting(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION, ["org_ehr"])
        super().setUp()

    @mock.patch("google.cloud.bigquery.Client")
    def test_core_data_datafeed(self, mock_bq):
        mock_bq_instance = mock_bq.return_value
        feed = InputFeed()

        query = feed.get_datafeed_definition("core data")['staging_data'].strip()
        staging_data_expected_sql = core_data_expected_sql.strip()
        streaming_data_expected_sql = core_data_expected_streaming_sql.strip()

        self.assertEqual(staging_data_expected_sql, query)

        feed.run_datafeed("core data")

        self.assertEqual(mock_bq_instance.query.call_count, 2)

        # Check the actual calls made
        expected_calls = [
            staging_data_expected_sql,
            streaming_data_expected_sql,
        ]
        actual_calls = [call.args[0].strip() for call in mock_bq_instance.query.mock_calls if call.args]

        for expected, actual in zip(expected_calls, actual_calls):
            self.assertEqual(expected, actual,
                             f"Mismatch in SQL call:\nExpected:\n{expected}\n\nActual:\n{actual}")

    @mock.patch("google.cloud.bigquery.Client")
    def test_biospecimen_datafeed(self, mock_bq):
        mock_bq_instance = mock_bq.return_value
        feed = InputFeed()

        query = feed.get_datafeed_definition("biospecimen")['staging_data'].strip()
        staging_data_expected_sql = biospecimen_expected_sql.strip()
        streaming_data_expected_sql = biospecimen_expected_streaming_sql.strip()

        self.assertEqual(staging_data_expected_sql, query)

        feed.run_datafeed("biospecimen")

        self.assertEqual(mock_bq_instance.query.call_count, 2)

        # Check the actual calls made
        expected_calls = [
            staging_data_expected_sql,
            streaming_data_expected_sql,
        ]
        actual_calls = [call.args[0].strip() for call in mock_bq_instance.query.mock_calls if call.args]

        for expected, actual in zip(expected_calls, actual_calls):
            self.assertEqual(expected, actual,
                             f"Mismatch in SQL call:\nExpected:\n{expected}\n\nActual:\n{actual}")

    @mock.patch("google.cloud.bigquery.Client")
    def test_ehr_datafeed(self, mock_bq):
        mock_bq_instance = mock_bq.return_value
        feed = InputFeed()

        query = feed.get_datafeed_definition("ehr")['staging_data'].strip()
        staging_data_expected_sql = ehr_expected_sql.strip()
        streaming_data_expected_sql = ehr_expected_streaming_sql.strip()

        self.assertEqual(staging_data_expected_sql, query)

        feed.run_datafeed("ehr")

        self.assertEqual(mock_bq_instance.query.call_count, 2)

        # Check the actual calls made
        expected_calls = [
            staging_data_expected_sql,
            streaming_data_expected_sql,
        ]
        actual_calls = [call.args[0].strip() for call in mock_bq_instance.query.mock_calls if call.args]

        for expected, actual in zip(expected_calls, actual_calls):
            self.assertEqual(expected, actual,
                             f"Mismatch in SQL call:\nExpected:\n{expected}\n\nActual:\n{actual}")

    @mock.patch("google.cloud.bigquery.Client")
    def test_health_data_sharing_datafeed(self, mock_bq):
        mock_bq_instance = mock_bq.return_value
        feed = InputFeed()

        query = feed.get_datafeed_definition("health data sharing")['staging_data'].strip()
        staging_data_expected_sql = health_data_sharing_expected_sql.strip()
        streaming_data_expected_sql = health_data_expected_streaming_sql.strip()

        self.assertEqual(staging_data_expected_sql, query)

        feed.run_datafeed("health data sharing")

        self.assertEqual(mock_bq_instance.query.call_count, 2)

        # Check the actual calls made
        expected_calls = [
            staging_data_expected_sql,
            streaming_data_expected_sql,
        ]
        actual_calls = [call.args[0].strip() for call in mock_bq_instance.query.mock_calls if call.args]

        for expected, actual in zip(expected_calls, actual_calls):
            self.assertEqual(expected, actual,
                             f"Mismatch in SQL call:\nExpected:\n{expected}\n\nActual:\n{actual}")

    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.InputFeed.make_datafeed_job")
    def test_run_datafeed(self, mock_make_datafeed_job, mock_bq_client):
        # Mock the BQ client to prevent API calls
        mock_bq_instance = mock_bq_client.return_value
        mock_bq_instance.query.return_value.result.return_value = []

        ppsc_data_gen = PPSCDataGenerator()
        test_participant = ppsc_data_gen.create_database_participant()
        # Test data
        mock_streaming_data_rows = [
            {"participant_id": test_participant.id, "event_date_time": "2024-11-20"},
        ]

        # Mock make_datafeed_job() to return the mocked streaming_data
        mock_make_datafeed_job.side_effect = lambda query: iter(
            mock_streaming_data_rows) if ehr_expected_streaming_sql in query else None

        # Test Feed
        feed = InputFeed()
        feed.get_datafeed_definition = mock.Mock(return_value={
            "staging_data": ehr_expected_sql,
            "streaming_data": ehr_expected_streaming_sql,
            "output_model": PPSCEHR
        })

        feed.run_datafeed("ehr")

        # Test insert into MySQL table worked
        ehr_dao = PPSCDataTransferBaseDao(PPSCEHR)
        actual_rows = ehr_dao.get_all()
        self.assertEqual(actual_rows[0].participant_id, mock_streaming_data_rows[0]['participant_id'])


class Intake2SummaryDataFeedTest(GenomicDataGenMixin):
    def setUp(self, *args, **kwargs) -> None:
        # pylint: disable=unused-argument
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        activities = [
            "ENROLLMENT",
            "Consent",
            "Survey Completion",
            "Profile Updates",
            "Withdrawal",
            "Deactivation",
            "Participant Status",
            "Attribution",
            "NPH Opt In"
        ]
        for activity in activities:
            self.ppsc_data_gen.create_database_activity(
                name=activity
            )

    def test_map_source_to_summary(self):
        record = {
            "participant_id": "348568008",
            "primary_consent": "Yes",
            "primary_consent_event_authored": "2024-11-21T18:12:00",
            "ehr_authorization": "No",
            "ehr_authorization_event_authored": "2024-11-20T15:30:00"
        }

        participant_summary = map_source_to_summary(record, consent_data_elements)

        self.assertEqual(participant_summary.participantId, "348568008")
        self.assertEqual(participant_summary.consentForStudyEnrollment,QuestionnaireStatus.SUBMITTED)
        self.assertEqual(participant_summary.consentForStudyEnrollmentAuthored,
                         "2024-11-21T18:12:00")
        self.assertEqual(participant_summary.consentForElectronicHealthRecords,
                         QuestionnaireStatus.SUBMITTED_NO_CONSENT)
        self.assertEqual(participant_summary.consentForElectronicHealthRecordsAuthored, "2024-11-20T15:30:00")

    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.Intake2SummaryFeed.make_datafeed_job")
    def test_consent_activity(self,  mock_make_datafeed_job, mock_bq_client):
        # Create requisite participant data
        ppsc_participant = self.ppsc_data_gen.create_database_participant()
        rdr_participant = self.data_generator.create_database_participant(participantId=ppsc_participant.id)
        self.data_generator.create_database_participant_summary(participant=rdr_participant)

        # Mock the BQ client to prevent API calls
        # Mock the BQ client to prevent API calls
        mock_bq_instance = mock_bq_client.return_value
        mock_bq_instance.query.return_value.result.return_value = []

        # Mock intake data
        activity_rows = [{
            "participant_id": ppsc_participant.id,
            "primary_consent": "Yes",
            "primary_consent_event_authored": "2024-11-21T18:12:00",
            "ehr_authorization": "No",
            "ehr_authorization_event_authored": "2024-11-20T15:30:00"
        }]

        # Mock make_datafeed_job to return the mocked intake data
        mock_make_datafeed_job.side_effect = lambda query: (
            iter(activity_rows) if query.strip() == consent_activity_expected_sql.strip() else None
        )

        # Test Feed
        feed = Intake2SummaryFeed()
        feed.get_datafeed_definition = mock.Mock(return_value={
                "source_data": consent_activity_expected_sql,
                "destination_model": ParticipantSummary,
                "de_mapping": consent_data_elements
            })

        feed.run_datafeed("Consent")

        ps_dao = ParticipantSummaryDao()
        actual_rows = ps_dao.get_all()
        self.assertEqual(actual_rows[0].participantId, activity_rows[0]['participant_id'])
        self.assertEqual(actual_rows[0].consentForStudyEnrollment, QuestionnaireStatus.SUBMITTED)
        self.assertEqual(actual_rows[0].consentForStudyEnrollmentAuthored,
                         datetime.datetime(2024, 11, 21, 18, 12))
        self.assertEqual(actual_rows[0].consentForElectronicHealthRecords, QuestionnaireStatus.SUBMITTED_NO_CONSENT)
        self.assertEqual(actual_rows[0].consentForElectronicHealthRecordsAuthored,
                         datetime.datetime(2024, 11, 20, 15, 30))


