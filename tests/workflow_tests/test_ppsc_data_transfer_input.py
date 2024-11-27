import datetime
from unittest import mock

from rdr_service import config
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.ppsc_partner_transfer_dao import PPSCDataTransferBaseDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.ppsc_partner_data_transfer import PPSCEHR
from rdr_service.participant_enums import QuestionnaireStatus, WithdrawalStatus, WithdrawalReason, SuspensionStatus, \
    DeceasedStatus, RetentionStatus, RetentionType, GenderIdentity, Race
from rdr_service.workflow_management.ppsc.ppsc_to_legacy_de_mappings import map_source_to_summary, \
    consent_data_elements, profile_updates_data_elements, withdrawal_data_elements, deactivation_data_elements, \
    participant_status_data_elements, survey_completion_data_elements, attribution_data_elements
from tests.service_tests.test_genomic_datagen import GenomicDataGenMixin
from tests.workflow_tests.test_data.ppsc_data_feed_test_data import core_data_expected_sql, biospecimen_expected_sql, \
    ehr_expected_sql, health_data_sharing_expected_sql, ehr_expected_streaming_sql, core_data_expected_streaming_sql, \
    biospecimen_expected_streaming_sql, health_data_expected_streaming_sql, consent_activity_expected_sql, \
    profile_updates_activity_expected_sql, withdrawal_activity_expected_sql, deactivation_activity_expected_sql, \
    participant_status_activity_expected_sql, survey_completion_activity_expected_sql, attribution_activity_expected_sql
from rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed import InputFeed, Intake2SummaryFeed


class DataTransferInputTest(GenomicDataGenMixin):
    def setUp(self, *args, **kwargs) -> None:
        # pylint: disable=unused-argument
        self.temporarily_override_config_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, ["participant_ehr"])
        self.temporarily_override_config_setting(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION, ["org_ehr"])
        super().setUp()

    def tearDown(self):
        self.clear_table_after_test("ppsc.participant")

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

    def tearDown(self):
        self.clear_table_after_test("rdr.participant_summary")
        self.clear_table_after_test("rdr.participant")
        self.clear_table_after_test("ppsc.participant")

    def create_required_codes(self, required_codes):
        for code in required_codes:
            self.data_generator.create_database_code(
                codeId=code
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
        self.assertEqual(participant_summary.consentForStudyEnrollment, QuestionnaireStatus.SUBMITTED)
        self.assertEqual(participant_summary.consentForStudyEnrollmentAuthored,
                         "2024-11-21T18:12:00")
        self.assertEqual(participant_summary.consentForElectronicHealthRecords,
                         QuestionnaireStatus.SUBMITTED_NO_CONSENT)
        self.assertEqual(participant_summary.consentForElectronicHealthRecordsAuthored, "2024-11-20T15:30:00")

    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch(
        "rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.Intake2SummaryFeed.make_datafeed_job")
    def test_consent_activity(self, mock_make_datafeed_job, mock_bq_client):
        # Create requisite participant data
        ppsc_participant = self.ppsc_data_gen.create_database_participant(id=110110110)
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

    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch(
        "rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.Intake2SummaryFeed.make_datafeed_job")
    def test_profile_updates_activity(self, mock_make_datafeed_job, mock_bq_client):
        # States
        required_codes = [631,632,633,634,635,636,637,638,639,640,641,642,643,644,645,646,647,648,649,650,651,652,653,
                          654,655,656,657,658,659,660,661,662,663,664,665,666,667,668,669,670,671,672,673,674,675,676,
                          677,678,679,680,681,682,683,684,685,686,687,688,689]
        self.create_required_codes(required_codes)

        # Create requisite participant data
        ppsc_participant = self.ppsc_data_gen.create_database_participant(id=110110111)
        rdr_participant = self.data_generator.create_database_participant(participantId=ppsc_participant.id)
        self.data_generator.create_database_participant_summary(participant=rdr_participant)

        # Mock the BQ client to prevent API calls
        mock_bq_instance = mock_bq_client.return_value
        mock_bq_instance.query.return_value.result.return_value = []

        # Mock intake data
        activity_rows = [{
            "participant_id": ppsc_participant.id,
            "piiname_first": "John",
            "piiname_middle": "A.",
            "piiname_last": "Doe",
            "streetaddress_piizip": "12345",
            "streetaddress_piistate": "PIIState_CA",
            "streetaddress_piicity": "San Francisco",
            "piiaddress_streetaddress": "123 Main St",
            "piiaddress_streetaddress2": "Apt 4B",
            "piicontactinformation_phone": "555-123-4567",
            "piicontactinformation_email": "johndoe@example.com",
            "language_preference": "English",
            "piibirthinformation_birthdate": "1985-06-15"
        }, {
            "participant_id": ppsc_participant.id,
            "piiname_first": None,
            "piiname_middle": None,
            "piiname_last": None,
            "streetaddress_piizip": None,
            "streetaddress_piistate": "PIIState_CA",
            "streetaddress_piicity": None,
            "piiaddress_streetaddress": None,
            "piiaddress_streetaddress2": None,
            "piicontactinformation_phone": None,
            "piicontactinformation_email": None,
            "language_preference": None,
            "piibirthinformation_birthdate": "1985-06-26"
        }]

        # Mock make_datafeed_job to return the mocked intake data
        mock_make_datafeed_job.side_effect = lambda query: (
            iter(activity_rows) if query.strip() == profile_updates_activity_expected_sql.strip() else None
        )

        # Test Feed
        feed = Intake2SummaryFeed()
        feed.get_datafeed_definition = mock.Mock(return_value={
            "source_data": profile_updates_activity_expected_sql,
            "destination_model": ParticipantSummary,
            "de_mapping": profile_updates_data_elements
        })

        feed.run_datafeed("Profile Updates")

        # Verify the database records
        ps_dao = ParticipantSummaryDao()
        actual_rows = ps_dao.get_all()

        # Assertions
        self.assertEqual(actual_rows[0].participantId, activity_rows[0]['participant_id'])
        self.assertEqual(actual_rows[0].firstName, "John")
        self.assertEqual(actual_rows[0].middleName, "A.")
        self.assertEqual(actual_rows[0].lastName, "Doe")
        self.assertEqual(actual_rows[0].zipCode, "12345")
        self.assertEqual(actual_rows[0].stateId, 672)
        self.assertEqual(actual_rows[0].city, "San Francisco")
        self.assertEqual(actual_rows[0].streetAddress, "123 Main St")
        self.assertEqual(actual_rows[0].streetAddress2, "Apt 4B")
        self.assertEqual(actual_rows[0].phoneNumber, "555-123-4567")
        self.assertEqual(actual_rows[0].email, "johndoe@example.com")
        self.assertEqual(actual_rows[0].primaryLanguage, "English")
        self.assertEqual(actual_rows[0].dateOfBirth, datetime.date(1985, 6, 26))

    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch(
        "rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.Intake2SummaryFeed.make_datafeed_job")
    def test_withdrawal_activity(self, mock_make_datafeed_job, mock_bq_client):
        # Create requisite participant data
        ppsc_participant = self.ppsc_data_gen.create_database_participant(id=110110112)
        rdr_participant = self.data_generator.create_database_participant(participantId=ppsc_participant.id)
        self.data_generator.create_database_participant_summary(participant=rdr_participant)

        # Mock the BQ client to prevent API calls
        mock_bq_instance = mock_bq_client.return_value
        mock_bq_instance.query.return_value.result.return_value = []

        # Mock intake data
        activity_rows = [{
            "participant_id": ppsc_participant.id,
            "withdrawal_status": "withdrawn",
            "withdrawal_status_authored_time": "2024-11-21T18:12:00",
            "withdrawal_reason": "Duplicate Account"
        }]

        # Mock make_datafeed_job to return the mocked intake data
        mock_make_datafeed_job.side_effect = lambda query: (
            iter(activity_rows) if query.strip() == withdrawal_activity_expected_sql.strip() else None
        )

        # Test Feed
        feed = Intake2SummaryFeed()
        feed.get_datafeed_definition = mock.Mock(return_value={
            "source_data": withdrawal_activity_expected_sql,
            "destination_model": ParticipantSummary,
            "de_mapping": withdrawal_data_elements
        })

        feed.run_datafeed("Withdrawal")

        # Verify the database records
        ps_dao = ParticipantSummaryDao()
        actual_rows = ps_dao.get_all()

        # Assertions
        self.assertEqual(actual_rows[0].participantId, activity_rows[0]['participant_id'])
        self.assertEqual(actual_rows[0].withdrawalStatus, WithdrawalStatus.NO_USE)
        self.assertEqual(actual_rows[0].withdrawalAuthored, datetime.datetime(2024, 11, 21, 18, 12))
        self.assertEqual(actual_rows[0].withdrawalReason, WithdrawalReason.DUPLICATE)

    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch(
        "rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.Intake2SummaryFeed.make_datafeed_job")
    def test_deactivation_activity(self, mock_make_datafeed_job, mock_bq_client):
        # Create requisite participant data
        ppsc_participant = self.ppsc_data_gen.create_database_participant(id=110110113)
        rdr_participant = self.data_generator.create_database_participant(participantId=ppsc_participant.id)
        self.data_generator.create_database_participant_summary(participant=rdr_participant)

        # Mock the BQ client to prevent API calls
        mock_bq_instance = mock_bq_client.return_value
        mock_bq_instance.query.return_value.result.return_value = []

        # Mock intake data
        activity_rows = [{
            "participant_id": ppsc_participant.id,
            "deactivation_status": "deactivated",
            "deactivation_status_time": "2024-11-22T12:45:00"
        }]

        # Mock make_datafeed_job to return the mocked intake data
        mock_make_datafeed_job.side_effect = lambda query: (
            iter(activity_rows) if query.strip() == deactivation_activity_expected_sql.strip() else None
        )

        # Test Feed
        feed = Intake2SummaryFeed()
        feed.get_datafeed_definition = mock.Mock(return_value={
            "source_data": deactivation_activity_expected_sql,
            "destination_model": ParticipantSummary,
            "de_mapping": deactivation_data_elements
        })

        feed.run_datafeed("Deactivation")

        # Verify the database records
        ps_dao = ParticipantSummaryDao()
        actual_rows = ps_dao.get_all()

        # Assertions
        self.assertEqual(actual_rows[0].participantId, activity_rows[0]['participant_id'])
        self.assertEqual(actual_rows[0].suspensionStatus, SuspensionStatus.NO_CONTACT)
        self.assertEqual(actual_rows[0].suspensionTime, datetime.datetime(2024, 11, 22, 12, 45))

    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch(
        "rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.Intake2SummaryFeed.make_datafeed_job")
    def test_participant_status_activity(self, mock_make_datafeed_job, mock_bq_client):
        # Create requisite participant data
        ppsc_participant = self.ppsc_data_gen.create_database_participant(id=110110114)
        rdr_participant = self.data_generator.create_database_participant(participantId=ppsc_participant.id)
        self.data_generator.create_database_participant_summary(participant=rdr_participant)

        # Mock the BQ client to prevent API calls
        mock_bq_instance = mock_bq_client.return_value
        mock_bq_instance.query.return_value.result.return_value = []

        # Mock intake data
        activity_rows = [{
            "participant_id": ppsc_participant.id,
            "test_account": "test",
            "deceased_status": "deceased",
            "deceased_authored": "2024-11-22T14:30:00",
            "retention_eligible_status": "eligible",
            "retention_eligible_status_authored": "2024-11-21T12:00:00",
            "retention_type": "active",
            "participant_time": "2024-11-20T08:00:00",
            "participant_ehr_consent_time": "2024-11-20T09:00:00",
            "enrolled_time": "2024-11-20T10:00:00",
            "pmb_eligible_time": "2024-11-20T11:00:00",
            "core_minus_pm_time": "2024-11-20T12:00:00",
            "core_participant_time": "2024-11-20T13:00:00"
        }]

        # Mock make_datafeed_job to return the mocked intake data
        mock_make_datafeed_job.side_effect = lambda query: (
            iter(activity_rows) if query.strip() == participant_status_activity_expected_sql.strip() else None
        )

        # Test Feed
        feed = Intake2SummaryFeed()
        feed.get_datafeed_definition = mock.Mock(return_value={
            "source_data": participant_status_activity_expected_sql,
            "destination_model": ParticipantSummary,
            "de_mapping": participant_status_data_elements
        })

        feed.run_datafeed("Participant Status")

        # Verify the database records
        ps_dao = ParticipantSummaryDao()
        actual_rows = ps_dao.get_all()

        participant_dao = ParticipantDao()
        test_participant = participant_dao.get(activity_rows[0]['participant_id'])

        # Assertions
        self.assertEqual(actual_rows[0].participantId, activity_rows[0]['participant_id'])
        self.assertTrue(test_participant.isTestParticipant)
        self.assertEqual(actual_rows[0].deceasedStatus, DeceasedStatus.APPROVED)
        self.assertEqual(actual_rows[0].deceasedAuthored, datetime.datetime(2024, 11, 22, 14, 30))
        self.assertEqual(actual_rows[0].retentionEligibleStatus, RetentionStatus.ELIGIBLE)
        self.assertEqual(actual_rows[0].retentionEligibleTime, datetime.datetime(2024, 11, 21, 12, 0))
        self.assertEqual(actual_rows[0].retentionType, RetentionType.ACTIVE)
        self.assertEqual(actual_rows[0].enrollmentStatusParticipantV3_2Time, datetime.datetime(2024, 11, 20, 8, 0))
        self.assertEqual(actual_rows[0].enrollmentStatusParticipantPlusEhrV3_2Time,
                         datetime.datetime(2024, 11, 20, 9, 0))
        self.assertEqual(actual_rows[0].enrollmentStatusEnrolledParticipantV3_2Time,
                         datetime.datetime(2024, 11, 20, 10, 0))
        self.assertEqual(actual_rows[0].enrollmentStatusPmbEligibleV3_2Time, datetime.datetime(2024, 11, 20, 11, 0))
        self.assertEqual(actual_rows[0].enrollmentStatusCoreMinusPmV3_2Time, datetime.datetime(2024, 11, 20, 12, 0))
        self.assertEqual(actual_rows[0].enrollmentStatusCoreV3_2Time, datetime.datetime(2024, 11, 20, 13, 0))

    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch(
        "rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.Intake2SummaryFeed.make_datafeed_job")
    def test_survey_completion_activity(self, mock_make_datafeed_job, mock_bq_client):
        required_codes = [302, 303, 301, 304, 924307, 311, 310, 309, 308, 39, 33, 36, 35,
                          38, 32, 37, 34, 292, 291, 297, 293, 294, 295, 290, 296, 298]
        self.create_required_codes(required_codes)
        # Create requisite participant data
        ppsc_participant = self.ppsc_data_gen.create_database_participant(id=110110116)
        rdr_participant = self.data_generator.create_database_participant(participantId=ppsc_participant.id)
        self.data_generator.create_database_participant_summary(participant=rdr_participant)

        # Mock the BQ client to prevent API calls
        mock_bq_instance = mock_bq_client.return_value
        mock_bq_instance.query.return_value.result.return_value = []

        # Mock intake data
        activity_rows = [{
            "participant_id": ppsc_participant.id,
            "gender_identity": "GenderIdentity_Man",
            "sex": "SexAtBirth_Male",
            "sexual_orientation": "SexualOrientation_Straight",
            "race": "WhatRaceEthnicity_Asian",
            "education": "HighestGrade_CollegeGraduate",
            "income": "AnnualIncome_50k75k",
            "aian": "yes",
            "questionnaire_on_overall_health": "submitted_complete",
            "questionnaire_on_overall_health_authored": "2024-11-22T14:30:00",
            "questionnaire_on_lifestyle": "submitted_incomplete",
            "questionnaire_on_lifestyle_authored": "2024-11-21T10:15:00",
            "questionnaire_on_the_basics": "submitted_complete",
            "questionnaire_on_the_basics_authored": "2024-11-20T08:00:00",
            "questionnaire_on_healthcare_access": "submitted_incomplete",
            "questionnaire_on_healthcare_access_authored": "2024-11-20T09:00:00",
            "questionnaire_on_social_determinants_of_health": "submitted_complete",
            "questionnaire_on_social_determinants_of_health_authored": "2024-11-20T11:00:00",
            "questionnaire_on_personal_and_family_health_history": "submitted_incomplete",
            "questionnaire_on_personal_and_family_health_history_authored": "2024-11-20T12:00:00",
            "questionnaire_on_life_functioning": "submitted_complete",
            "questionnaire_on_life_functioning_authored": "2024-11-20T13:00:00",
            "questionnaire_on_emotional_health_history_and_well_being": "submitted_complete",
            "questionnaire_on_emotional_health_history_and_well_being_authored": "2024-11-20T14:00:00",
            "questionnaire_on_behavioral_health_and_personality": "submitted_incomplete",
            "questionnaire_on_behavioral_health_and_personality_authored": "2024-11-20T15:00:00",
            # TODO Skipping Pediatric for now
            # "questionnaire_on_environmental_exposures": "submitted_complete",
            # "questionnaire_on_environmental_exposures_authored": "2024-11-20T16:00:00"
        }]

        # Mock make_datafeed_job to return the mocked intake data
        mock_make_datafeed_job.side_effect = lambda query: (
            iter(activity_rows) if query.strip() == survey_completion_activity_expected_sql.strip() else None
        )

        # Test Feed
        feed = Intake2SummaryFeed()
        feed.get_datafeed_definition = mock.Mock(return_value={
            "source_data": survey_completion_activity_expected_sql,
            "destination_model": ParticipantSummary,
            "de_mapping": survey_completion_data_elements
        })

        feed.run_datafeed("Survey Completion")

        # Verify the database records
        ps_dao = ParticipantSummaryDao()
        actual_rows = ps_dao.get_all()

        # Assertions for Basics Data
        self.assertEqual(actual_rows[0].participantId, activity_rows[0]['participant_id'])
        self.assertEqual(actual_rows[0].genderIdentity, GenderIdentity.GenderIdentity_Man)
        self.assertEqual(actual_rows[0].sexId, 302)
        self.assertEqual(actual_rows[0].sexualOrientationId, 308)
        self.assertEqual(actual_rows[0].race, Race.ASIAN)
        self.assertEqual(actual_rows[0].educationId, 33)
        self.assertEqual(actual_rows[0].incomeId, 295)
        self.assertEqual(actual_rows[0].aian, 1)

        # Assertions for Overall Health
        self.assertEqual(actual_rows[0].questionnaireOnOverallHealth, QuestionnaireStatus.SUBMITTED)
        self.assertEqual(actual_rows[0].questionnaireOnOverallHealthAuthored, datetime.datetime(2024, 11, 22, 14, 30))

        # Assertions for Lifestyle
        self.assertIsNone(actual_rows[0].questionnaireOnLifestyle)
        self.assertEqual(actual_rows[0].questionnaireOnLifestyleAuthored, datetime.datetime(2024, 11, 21, 10, 15))

        # Assertions for The Basics
        self.assertEqual(actual_rows[0].questionnaireOnTheBasics, QuestionnaireStatus.SUBMITTED)
        self.assertEqual(actual_rows[0].questionnaireOnTheBasicsAuthored, datetime.datetime(2024, 11, 20, 8, 0))

        # Assertions for Health Care Access
        self.assertIsNone(actual_rows[0].questionnaireOnHealthcareAccess)
        self.assertEqual(actual_rows[0].questionnaireOnHealthcareAccessAuthored, datetime.datetime(2024, 11, 20, 9, 0))

        # Assertions for Social Determinants
        self.assertEqual(actual_rows[0].questionnaireOnSocialDeterminantsOfHealth, QuestionnaireStatus.SUBMITTED)
        self.assertEqual(actual_rows[0].questionnaireOnSocialDeterminantsOfHealthAuthored,
                         datetime.datetime(2024, 11, 20, 11, 0))

        # Assertions for Personal and Family Health History
        self.assertIsNone(actual_rows[0].questionnaireOnPersonalAndFamilyHealthHistory)
        self.assertEqual(actual_rows[0].questionnaireOnPersonalAndFamilyHealthHistoryAuthored,
                         datetime.datetime(2024, 11, 20, 12, 0))

        # Assertions for Life Functioning
        self.assertEqual(actual_rows[0].questionnaireOnLifeFunctioning, QuestionnaireStatus.SUBMITTED)
        self.assertEqual(actual_rows[0].questionnaireOnLifeFunctioningAuthored, datetime.datetime(2024, 11, 20, 13, 0))

        # Assertions for Emotional Health
        self.assertEqual(actual_rows[0].questionnaireOnEmotionalHealthHistoryAndWellBeing,
                         QuestionnaireStatus.SUBMITTED)
        self.assertEqual(actual_rows[0].questionnaireOnEmotionalHealthHistoryAndWellBeingAuthored,
                         datetime.datetime(2024, 11, 20, 14, 0))

        # Assertions for Behavioral Health
        self.assertIsNone(actual_rows[0].questionnaireOnBehavioralHealthAndPersonality)
        self.assertEqual(actual_rows[0].questionnaireOnBehavioralHealthAndPersonalityAuthored,
                         datetime.datetime(2024, 11, 20, 15, 0))

        # Assertions for Pediatric Environmental Health
        # self.assertEqual(actual_rows[0].questionnaireOnEnvironmentalExposures, QuestionnaireStatus.SUBMITTED)
        # self.assertEqual(actual_rows[0].questionnaireOnEnvironmentalExposuresAuthored,
        #                  datetime.datetime(2024, 11, 20, 16, 0))

    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch(
        "rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.Intake2SummaryFeed.make_datafeed_job")
    def test_attribution_activity(self, mock_make_datafeed_job, mock_bq_client):
        # Create requisite participant data
        ppsc_participant = self.ppsc_data_gen.create_database_participant(id=110110120)
        rdr_participant = self.data_generator.create_database_participant(participantId=ppsc_participant.id)
        self.data_generator.create_database_participant_summary(participant=rdr_participant)

        # Mock the BQ client to prevent API calls
        mock_bq_instance = mock_bq_client.return_value
        mock_bq_instance.query.return_value.result.return_value = []

        # Mock intake data
        activity_rows = [{
            "participant_id": ppsc_participant.id,
            "organization": "PITT_BANNER_HEALTH"
        }]

        # Mock make_datafeed_job to return the mocked intake data
        mock_make_datafeed_job.side_effect = lambda query: (
            iter(activity_rows) if query.strip() == attribution_activity_expected_sql.strip() else None
        )

        # Test Feed
        feed = Intake2SummaryFeed()
        feed.get_datafeed_definition = mock.Mock(return_value={
            "source_data": attribution_activity_expected_sql,
            "destination_model": ParticipantSummary,
            "de_mapping": attribution_data_elements
        })

        feed.run_datafeed("Attribution")

        # Verify the database records
        ps_dao = ParticipantSummaryDao()
        actual_rows = ps_dao.get_all()

        # Assertions for Attribution Data
        self.assertEqual(actual_rows[0].participantId, activity_rows[0]['participant_id'])
        self.assertEqual(actual_rows[0].organizationId, 3)
