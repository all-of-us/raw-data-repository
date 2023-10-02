from datetime import datetime, timedelta
import mock
from pprint import pprint

from rdr_service import config
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.offline.retention_eligible_import import RetentionEligibility, _supplement_with_rdr_calculations
from rdr_service.participant_enums import QuestionnaireStatus, EhrStatus, RetentionStatus, RetentionType
from rdr_service.services.retention_calculation import RetentionEligibilityDependencies, Consent
from tests.helpers.unittest_base import BaseTestCase


class RetentionCalculationIntegrationTest(BaseTestCase):
    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
        self.participant = self.data_generator.create_database_participant()
        self.first_ehr_intent_timestamp = datetime(2020, 1, 1, 0, 15, 0)
        # Boundary date for maintaining active retention, tests can create dates with deltas relative to this
        self.date_18_months_ago = RetentionEligibility._get_datetime_18_months_ago()
        # Mocking the return for the earliest EHR intent so QuestionnaireResponse data setup is not needed
        first_ehr_consent_patch = mock.patch(
            'rdr_service.offline.retention_eligible_import._get_earliest_intent_for_ehr',
            return_value=Consent(is_consent_provided=True,
                                 authored_timestamp=self.first_ehr_intent_timestamp)
        )
        self.retention_calc_mock = first_ehr_consent_patch.start()
        self.addCleanup(first_ehr_consent_patch.stop)

    def _create_retention_eligible_participant(self, **kwargs):
        """
         Create a participant summary with the activity details needed to be considered retention eligible, for
         testing _supplement_with_rdr_calculations() results
        """
        # Set up defaults
        ps_args = {
            'consentCohort': 3,
            'dateOfBirth': datetime(1982, 1, 9),
            'consentForStudyEnrollmentFirstYesAuthored': datetime(2020, 1, 1, 0, 0, 0),
            'consentForStudyEnrollmentAuthored': datetime(2020, 1, 1, 0, 0, 0),
            'consentForStudyEnrollment': QuestionnaireStatus.SUBMITTED,
            'consentForElectronicHealthRecordsFirstYesAuthored': self.first_ehr_intent_timestamp,
            'consentForElectronicHealthRecords': QuestionnaireStatus.SUBMITTED,
            'consentForElectronicHealthRecordsAuthored': self.first_ehr_intent_timestamp,
            'questionnaireOnTheBasics': QuestionnaireStatus.SUBMITTED,
            'questionnaireOnTheBasicsAuthored': datetime(2020, 1, 1, 1, 0, 0),
            'questionnaireOnOverallHealth': QuestionnaireStatus.SUBMITTED,
            'questionnaireOnOverallHealthAuthored': datetime(2020, 1, 1, 2, 0, 0),
            'questionnaireOnLifestyle': QuestionnaireStatus.SUBMITTED,
            'questionnaireOnLifestyleAuthored': datetime(2020, 1, 1, 3, 0, 0)
        }
        # Apply caller-provided values
        if kwargs:
            ps_args.update(**kwargs)
        eligible_pid_summary = self.data_generator.create_database_participant_summary(**ps_args)

        self.data_generator.create_database_biobank_stored_sample(
            test='1ED04',
            biobankId=eligible_pid_summary.biobankId,
            confirmed=datetime(2020, 3, 4)
        )
        return eligible_pid_summary

    @mock.patch('rdr_service.offline.retention_eligible_import.RetentionEligibility')
    def test_get_earliest_dna_sample(self, calc_mock):
        # mock the retention calculation to see what it got passed
        self.data_generator.create_database_participant_summary(participant=self.participant)
        self.temporarily_override_config_setting(
            key=config.DNA_SAMPLE_TEST_CODES,
            value=['1ED04', '1SAL2']
        )

        first_dna_sample_timestamp = datetime(2020, 3, 4)
        for test, timestamp in [
            ('not_dna', datetime(2019, 1, 19)),
            ('1ED04', first_dna_sample_timestamp),
            ('1SAL2', datetime(2021, 4, 2))
        ]:
            self.data_generator.create_database_biobank_stored_sample(
                test=test,
                biobankId=self.participant.biobankId,
                confirmed=timestamp
            )

        retention_parameters = self._get_retention_dependencies_found(mock_obj=calc_mock)
        self.assertEqual(first_dna_sample_timestamp, retention_parameters.dna_samples_timestamp)

    def test_mhwb_survey_activity(self):
        ehhwb_survey_authored = self.date_18_months_ago + timedelta(300)
        bhp_survey_authored = self.date_18_months_ago + timedelta(310)
        # DA-3705:  Confirm the RetentionMetricsEligibility picks up new values for MHWB survey activity
        ehhwb_summary = self._create_retention_eligible_participant(
            participantId=self.participant.participantId,
            questionnaireOnEmotionalHealthHistoryAndWellBeing=QuestionnaireStatus.SUBMITTED,
            questionnaireOnEmotionalHealthHistoryAndWellBeingAuthored=ehhwb_survey_authored
        )
        self._assert_expected_last_retention_activity_time(
            ehhwb_summary,
            ehhwb_summary.questionnaireOnEmotionalHealthHistoryAndWellBeingAuthored
        )
        bhp_summary = self._create_retention_eligible_participant(
            questionnaireOnBehavioralHealthAndPersonality=QuestionnaireStatus.SUBMITTED,
            questionnaireOnBehavioralHealthAndPersonalityAuthored=bhp_survey_authored
        )
        self._assert_expected_last_retention_activity_time(
            bhp_summary,
            bhp_summary.questionnaireOnBehavioralHealthAndPersonalityAuthored
        )

    def test_nph1_consent_activity(self):
        # Note:   this is theoretical, since currently, there are no participants in the RDR participant_summary in
        # production who have a consentForNphModule1Authored timestamp but have a "false" consented status ("no"
        # consents not recorded)
        nph_mod1_authored = self.date_18_months_ago + timedelta(300)
        nph_summary = self._create_retention_eligible_participant(
            participantId=self.participant.participantId,
            consentForNphModule1Authored=nph_mod1_authored,
            consentForNphModule1=True
        )
        self._assert_expected_last_retention_activity_time(nph_summary, nph_summary.consentForNphModule1Authored)

    def test_wear_consent_activity(self):
        # Test both yes and no responses for WEAR study are recognized for retention
        wear_yes_authored = self.date_18_months_ago + timedelta(150)
        wear_yes_summary = self._create_retention_eligible_participant(
            participantId=self.participant.participantId,
            consentForWearStudy=QuestionnaireStatus.SUBMITTED,
            consentForWearStudyAuthored=wear_yes_authored
        )
        self._assert_expected_last_retention_activity_time(wear_yes_summary,
                                                           wear_yes_summary.consentForWearStudyAuthored)

        wear_no_authored = self.date_18_months_ago + timedelta(200)
        wear_no_summary = self._create_retention_eligible_participant(
            consentForWearStudy=QuestionnaireStatus.SUBMITTED_NO_CONSENT,
            consentForWearStudyAuthored=wear_no_authored
        )
        self._assert_expected_last_retention_activity_time(wear_no_summary, wear_no_summary.consentForWearStudyAuthored)

    def test_etm_consent_activity(self):
        # EtM consent more recent than WEAR consent
        wear_yes_authored = self.date_18_months_ago + timedelta(250)
        etm_yes_authored = self.date_18_months_ago + timedelta(300)
        etm_yes_summary = self._create_retention_eligible_participant(
            participantId=self.participant.participantId,
            consentForWearStudy=QuestionnaireStatus.SUBMITTED_NO_CONSENT,
            consentForWearStudyAuthored=wear_yes_authored,
            consentForEtM=QuestionnaireStatus.SUBMITTED,
            consentForEtMAuthored=etm_yes_authored
        )
        self._assert_expected_last_retention_activity_time(etm_yes_summary, etm_yes_summary.consentForEtMAuthored)

        # EtM "no" more recent than WEAR "yes", confirm recognition of "no" EtM consent responses as latest
        etm_no_authored = self.date_18_months_ago + timedelta(320)
        etm_no_summary = self._create_retention_eligible_participant(
            consentForWearStudy=QuestionnaireStatus.SUBMITTED,
            consentForWearStudyAuthored=wear_yes_authored,
            consentForEtM=QuestionnaireStatus.SUBMITTED_NO_CONSENT,
            consentForEtMAuthored=etm_no_authored
        )
        self._assert_expected_last_retention_activity_time(etm_no_summary, etm_no_summary.consentForEtMAuthored)

    def test_etm_task_activity(self):
        # EtM task as most recent activity
        surveys_authored = self.date_18_months_ago + timedelta(300)
        etm_task_authored = self.date_18_months_ago + timedelta(330)
        summary = self._create_retention_eligible_participant(
            participantId=self.participant.participantId,
            questionnaireOnEmotionalHealthHistoryAndWellBeingAuthored=surveys_authored,
            questionnaireOnBehavioralHealthAndPersonalityAuthored=surveys_authored,
            latestEtMTaskAuthored=etm_task_authored
        )
        self._assert_expected_last_retention_activity_time(summary, summary.latestEtMTaskAuthored)

    def test_no_longer_actively_retained(self):
        # Set latest activity date to 18 months + 1 day ago,  to test for not actively retained
        sdoh_authored = self.date_18_months_ago - timedelta(1)
        summary = self._create_retention_eligible_participant(
            participantId=self.participant.participantId,
            questionnaireOnSocialDeterminantsOfHealthAuthored=sdoh_authored
        )
        obj = self._make_retention_eligible_metrics_rec(summary)
        _supplement_with_rdr_calculations(
            obj,
            session=self.session
        )
        self.assertEqual(obj.rdr_is_actively_retained, False)

    def test_passively_retained(self):
        # Passive retention is based on last EHR upload being within last 18 months
        ehr_upload = self.date_18_months_ago + timedelta(1)
        summary = self._create_retention_eligible_participant(
            participantId=self.participant.participantId,
            ehrStatus=EhrStatus.PRESENT,
            ehrUpdateTime=ehr_upload,
        )
        obj = self._make_retention_eligible_metrics_rec(summary)
        _supplement_with_rdr_calculations(
            obj,
            session=self.session
        )
        self.assertEqual(obj.rdr_is_passively_retained, True)

    def test_no_longer_passively_retained(self):
        # Make last EHR upload > 18 months ago (no longer passively retained)
        ehr_upload = self.date_18_months_ago - timedelta(1)
        summary = self._create_retention_eligible_participant(
            dateOfBirth=datetime(1982, 1, 9),
            consentForStudyEnrollmentFirstYesAuthored=datetime(2020, 1, 10),
            ehrStatus=EhrStatus.PRESENT,
            ehrUpdateTime=ehr_upload
        )
        obj = self._make_retention_eligible_metrics_rec(summary)
        _supplement_with_rdr_calculations(
            obj,
            session=self.session
        )
        self.assertEqual(obj.rdr_is_passively_retained, False)

    def _get_retention_dependencies_found(self, mock_obj=None, participant=None) -> RetentionEligibilityDependencies:
        """
        Call the code responsible for collecting the retention calculation data.
        Return the data in provided to the calculation code.
        """
        pid = participant.participantId if participant else self.participant.participantId
        retention_data = _supplement_with_rdr_calculations(
            RetentionEligibleMetrics(participantId=pid),
            session=self.session
        )
        if mock_obj:
            return mock_obj.call_args[0][0]

        return retention_data

    def _assert_expected_last_retention_activity_time(self, participant_summary, expected_date):
        """
        Invoke the retention calculation for the specified participant and confirm the expected active retention date
        """
        obj = self._make_retention_eligible_metrics_rec(participant_summary)
        _supplement_with_rdr_calculations(
            metrics_data=obj,
            session=self.session
        )
        print(f'Expected date: {expected_date}')
        pprint(obj.__dict__)
        self.assertEqual(obj.rdr_last_retention_activity_time, expected_date)

    def _make_retention_eligible_metrics_rec(self, summary):
        return RetentionEligibleMetrics(
            id=1,
            created=None,
            modified=None,
            participantId=summary.participantId,
            retentionEligible=True,
            retentionEligibleTime=summary.questionnaireOnLifestyleAuthored,
            lastActiveRetentionActivityTime=summary.questionnaireOnLifestyleAuthored,
            activelyRetained=False,
            passivelyRetained=False,
            fileUploadDate=None,
            retentionEligibleStatus=RetentionStatus.ELIGIBLE,
            retentionType=RetentionType.ACTIVE_AND_PASSIVE
        )
