from datetime import datetime

from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.participant_enums import WithdrawalStatus
from rdr_service.services.system_utils import DateRange
from tests.helpers.unittest_base import BaseTestCase


class TestRetentionTask(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

        self.test_client = None

    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)

        self._db_factory_mock = self.mock('rdr_service.dao.base_dao.database_factory')
        self.session_mock = self._db_factory_mock.get_database().session().__enter__()

        # When initialized a DAO automatically tries to connect to the database, and currently there's a task endpoint
        # that initializes a DAO as part of the class definition. So need to mock the db factory before setting up the
        # flask app.
        if not self.test_client:
            from rdr_service.resource.main import app
            self.test_client = app.test_client()

        self.existing_record_mock = self.mock(
            'rdr_service.dao.retention_eligible_metrics_dao.'
            'RetentionEligibleMetricsDao.get_existing_record'
        )
        self.existing_record_mock.return_value = None

        self._ehr_consent_mock = self.mock(
            'rdr_service.repository.questionnaire_response_repository.'
            'QuestionnaireResponseRepository.get_interest_in_sharing_ehr_ranges'
        )
        self._dna_sample_mock = self.mock(
            'rdr_service.dao.biobank_stored_sample_dao.'
            'BiobankStoredSampleDao.get_earliest_confirmed_dna_sample_timestamp'
        )
        self.summary_update_mock = self.mock(
            'rdr_service.dao.participant_summary_dao.'
            'ParticipantSummaryDao.update_with_retention_data'
        )

        self.temporarily_override_config_setting('enable_retention_calc_task', True)

    def test_basic_insert(self):
        """
        Trigger a retention task for a participant that doesn't have retention data yet.
        """
        generic_time = datetime.now()
        summary = ParticipantSummary(
            participantId=123123123,
            withdrawalStatus=WithdrawalStatus.NOT_WITHDRAWN,
            consentForStudyEnrollmentFirstYesAuthored=generic_time,
            questionnaireOnTheBasicsAuthored=generic_time,
            questionnaireOnOverallHealthAuthored=generic_time,
            questionnaireOnLifestyleAuthored=generic_time,
            wasEhrDataAvailable=True,
            ehrUpdateTime=generic_time
        )
        self._set_mock_summary(summary)
        self._set_ehr_consent_time(generic_time)
        self._set_earliest_sample_time(generic_time)

        self._create_retention_update_task(summary.participantId)
        added_metrics: RetentionEligibleMetrics = self.session_mock.add.call_args[0][0]
        self.assertTrue(added_metrics.rdr_retention_eligible)
        self.assertEqual(generic_time, added_metrics.rdr_retention_eligible_time)
        self.assertTrue(added_metrics.rdr_is_passively_retained)

    def test_basic_update(self):
        """
        Trigger a retention task for a participant that has retention data that needs updated
        """

        self.existing_record_mock.return_value = RetentionEligibleMetrics(
            rdr_retention_eligible=False
        )

        generic_time = datetime.now()
        summary = ParticipantSummary(
            participantId=123123123,
            withdrawalStatus=WithdrawalStatus.NOT_WITHDRAWN,
            consentForStudyEnrollmentFirstYesAuthored=generic_time,
            questionnaireOnTheBasicsAuthored=generic_time,
            questionnaireOnOverallHealthAuthored=generic_time,
            questionnaireOnLifestyleAuthored=generic_time,
            wasEhrDataAvailable=True,
            ehrUpdateTime=generic_time
        )
        self._set_mock_summary(summary)
        self._set_ehr_consent_time(generic_time)
        self._set_earliest_sample_time(generic_time)

        self._create_retention_update_task(summary.participantId)

        self.session_mock.add.assert_not_called()  # New retention data should not be added
        self.summary_update_mock.assert_called()  # But the participant summary should be updated

    def test_skipping_update(self):
        """
        Make sure the process skips any updating of anything if we already have the retention set correctly
        """
        generic_time = datetime.now()
        self.existing_record_mock.return_value = RetentionEligibleMetrics(
            rdr_retention_eligible=True,
            rdr_retention_eligible_time=generic_time,
            rdr_is_actively_retained=False,
            rdr_is_passively_retained=True
        )

        summary = ParticipantSummary(
            participantId=123123123,
            withdrawalStatus=WithdrawalStatus.NOT_WITHDRAWN,
            consentForStudyEnrollmentFirstYesAuthored=generic_time,
            questionnaireOnTheBasicsAuthored=generic_time,
            questionnaireOnOverallHealthAuthored=generic_time,
            questionnaireOnLifestyleAuthored=generic_time,
            wasEhrDataAvailable=True,
            ehrUpdateTime=generic_time
        )
        self._set_mock_summary(summary)
        self._set_ehr_consent_time(generic_time)
        self._set_earliest_sample_time(generic_time)

        self._create_retention_update_task(summary.participantId)

        self.session_mock.add.assert_not_called()  # New retention data should not be added
        self.summary_update_mock.assert_not_called()  # The summary does not need updated if nothing changed

    def _create_retention_update_task(self, participant_id):
        self.send_post(
            '/resource/task/UpdateRetentionStatus',
            {
                'participant_id': participant_id
            },
            test_client=self.test_client,
            prefix=''
        )

    def _set_mock_summary(self, summary: ParticipantSummary):
        self.session_mock.query().get.return_value = summary

    def _set_ehr_consent_time(self, consent_time):
        self._ehr_consent_mock.return_value = [DateRange(start=consent_time)]

    def _set_earliest_sample_time(self, confirmed_time):
        self._dna_sample_mock.return_value = confirmed_time
