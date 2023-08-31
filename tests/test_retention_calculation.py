from datetime import datetime
import mock

from rdr_service import config, code_constants
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.offline.retention_eligible_import import _supplement_with_rdr_calculations
from rdr_service.participant_enums import QuestionnaireResponseStatus, QuestionnaireResponseClassificationType
from rdr_service.services.retention_calculation import RetentionEligibilityDependencies
from tests.helpers.unittest_base import BaseTestCase


class RetentionCalculationIntegrationTest(BaseTestCase):
    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
        participant = self.data_generator.create_database_participant(participantOrigin='test_portal')
        self.summary = self.data_generator.create_database_participant_summary(
            participant=participant,
            dateOfBirth=datetime(1982, 1, 9),
            consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10),
            questionnaireOnEmotionalHealthHistoryAndWellBeingAuthored=datetime(2023, 5, 15),
            questionnaireOnBehavioralHealthAndPersonalityAuthored=datetime(2023, 6, 1)
        )

        self.wear_consent_question = self.data_generator.create_database_code(
            value=code_constants.WEAR_CONSENT_QUESTION_CODE
        )
        self.wear_yes = self.data_generator.create_database_code(value=code_constants.WEAR_YES_ANSWER_CODE)
        self.wear_no = self.data_generator.create_database_code(value=code_constants.WEAR_NO_ANSWER_CODE)

        self.etm_consent_question = self.data_generator.create_database_code(
            value=code_constants.ETM_CONSENT_QUESTION_CODE
        )
        self.etm_yes = self.data_generator.create_database_code(value=code_constants.AGREE_YES)
        self.etm_no = self.data_generator.create_database_code(value=code_constants.AGREE_NO)

        self.etm_consent = self._create_consent_questionnaire('etm_consent_ut', self.etm_consent_question.codeId)
        self.wear_consent = self._create_consent_questionnaire('wear_consent_ut', self.wear_consent_question.codeId)

        # mock the retention calculation to see what it got passed
        retention_calc_patch = mock.patch('rdr_service.offline.retention_eligible_import.RetentionEligibility')
        self.retention_calc_mock = retention_calc_patch.start()
        self.addCleanup(retention_calc_patch.stop)

    def _create_consent_questionnaire(self, module: str, consent_question_code: int):
        module_code = self.data_generator.create_database_code(value=module)

        questionnaire = self.data_generator.create_database_questionnaire_history()
        self.data_generator.create_database_questionnaire_question(
                questionnaireId=questionnaire.questionnaireId,
                questionnaireVersion=questionnaire.version,
                codeId=consent_question_code
            )

        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            codeId=module_code.codeId
        )

        return questionnaire

    def _setup_consent_questionnaire_response(self, participant, questionnaire, answer_code,
                                              authored=datetime(2023, 5, 15),
                                              created=datetime(2023, 3, 15),
                                              status=QuestionnaireResponseStatus.COMPLETED,
                                              classification_type=QuestionnaireResponseClassificationType.COMPLETE):

        questionnaire_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant,
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            authored=authored,
            created=created,
            status=status,
            classificationType=classification_type
        )

        question = questionnaire.questions[0]
        self.data_generator.create_database_questionnaire_response_answer(
                questionnaireResponseId=questionnaire_response.questionnaireResponseId,
                questionId=question.questionnaireQuestionId,
                **{'valueCodeId': answer_code.codeId}
            )

        return questionnaire_response

    def test_get_earliest_dna_sample(self):
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
                biobankId=self.summary.biobankId,
                confirmed=timestamp
            )

        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(first_dna_sample_timestamp, retention_parameters.dna_samples_timestamp)

    def test_mhwb_surveys_detected(self):
        # DA-3705:  Confirm the RetentionMetricsEligibility picks up new values for MHWB surveys
        # Check for the default participant_summary setup authored times
        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(self.summary.questionnaireOnBehavioralHealthAndPersonalityAuthored,
                         retention_parameters.bhp_response_timestamp)
        self.assertEqual(self.summary.questionnaireOnEmotionalHealthHistoryAndWellBeingAuthored,
                         retention_parameters.ehhwb_response_timestamp)

    def test_nph1_consent_status(self):
        # Test participant without an NPH1 authored time - use default paritcipant from test setup
        retention_parameters = self._get_retention_dependencies_found()
        self.assertIsNone(retention_parameters.nph_consent_timestamp)
        # Create/test participant with NPH1 consent authored
        nph_participant = self.data_generator.create_database_participant(participantOrigin='test_portal')
        nph_pid_summary = self.data_generator.create_database_participant_summary(
            participant=nph_participant,
            dateOfBirth=datetime(1982, 1, 9),
            consentForStudyEnrollmentFirstYesAuthored=datetime(2000, 1, 10),
            questionnaireOnEmotionalHealthHistoryAndWellBeingAuthored=datetime(2023, 5, 15),
            questionnaireOnBehavioralHealthAndPersonalityAuthored=datetime(2023, 6, 1),
            consentForNphModule1Authored=datetime(2023, 6, 30),
            consentForNphModule1=True
        )
        retention_parameters = self._get_retention_dependencies_found(participant=nph_participant)
        self.assertEqual(nph_pid_summary.consentForNphModule1Authored, retention_parameters.nph_consent_timestamp)

    def test_wear_yes_timestamp(self):
        # DA-3705: Confirm logic to find the WEAR consent response data
        qr = self._setup_consent_questionnaire_response(self.summary.participantId,
                                                        self.wear_consent, self.wear_yes)
        self.assertIsNotNone(qr)
        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(qr.authored, retention_parameters.wear_consent_timestamp)

    def test_wear_no_response_no_timestamp(self):
        # DA-3705:  Confirm WEAR consent "no" response is ignored
        qr = self._setup_consent_questionnaire_response(self.summary.participantId,
                                                        self.wear_consent, self.wear_no)
        self.assertIsNotNone(qr)
        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(None, retention_parameters.wear_consent_timestamp)

    def test_multiple_wear_responses_latest_yes(self):
        # DA-3705: Confirm if there are multiple WEAR consent responses, the latest "yes" is used
        self._setup_consent_questionnaire_response(self.summary.participantId,
                                                   self.wear_consent, self.wear_no)
        qr_no_1 = self._setup_consent_questionnaire_response(self.summary.participantId, self.wear_consent,
                                                             self.wear_no, authored=datetime(2023, 6, 1))
        qr_yes_2 = self._setup_consent_questionnaire_response(self.summary.participantId,  self.wear_consent,
                                                              self.wear_yes, authored=datetime(2023, 3, 1))
        self.assertTrue(None not in [qr_no_1, qr_yes_2])
        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(qr_yes_2.authored, retention_parameters.wear_consent_timestamp)

    def test_etm_consent_yes(self):
        # DA-3705: Confirm logic to find EtM consent response data
        qr_yes = self._setup_consent_questionnaire_response(self.summary.participantId, self.etm_consent, self.etm_yes,
                                                            authored=datetime(2023, 1, 1))
        self.assertIsNotNone(qr_yes)
        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(qr_yes.authored, retention_parameters.etm_consent_timestamp)

    def test_etm_consent_no_response_no_timestamp(self):
        # DA-3705: Confirm if EtM consent is "no" response, it's ignored
        no_rsp = self._setup_consent_questionnaire_response(self.summary.participantId, self.etm_consent, self.etm_no,
                                                            authored=datetime(2023, 1, 1))
        self.assertIsNotNone(no_rsp)
        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(None, retention_parameters.etm_consent_timestamp)

    def test_multiple_etm_consent_yes_latest(self):
        # DA-3705: Confirm if multiple EtM consent "yes" responses, latest authored is used
        qr_first_yes = self._setup_consent_questionnaire_response(self.summary.participantId, self.etm_consent,
                                                                  self.etm_yes, authored=datetime(2023, 1, 1))
        qr_later_yes = self._setup_consent_questionnaire_response(self.summary.participantId, self.etm_consent,
                                                                  self.etm_yes, authored=datetime(2023, 3, 1))
        self.assertTrue(None not in [qr_first_yes, qr_later_yes])
        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(qr_later_yes.authored, retention_parameters.etm_consent_timestamp)

    def test_etm_task_timestamp(self):
        # DA-3705: Confirm logic to find latest EtM task response authored.  Need EtM consent to process task
        self._setup_consent_questionnaire_response(self.summary.participantId, self.etm_consent, self.etm_yes,
                                                   authored=datetime(2023, 1, 1))
        # Default EtM questionnaire created is emorecog
        etm_rsp_1 = self.data_generator.create_etm_questionnaire_response(
            authored=datetime(2023, 3, 1),
            participant_id=self.summary.participantId
        )
        self.assertIsNotNone(etm_rsp_1)
        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(etm_rsp_1.authored, retention_parameters.latest_etm_response_timestamp)

    def test_etm_multiple_tasks_latest(self):
        self._setup_consent_questionnaire_response(self.summary.participantId, self.etm_consent, self.etm_yes,
                                                   authored=datetime(2023, 1, 1))
        q_emorecog = self.data_generator.create_database_etm_questionnaire(questionnaire_type='emorecog')
        q_flanker = self.data_generator.create_database_etm_questionnaire(questionnaire_type='flanker')
        emorecog_rsp = self.data_generator.create_etm_questionnaire_response(
            participant_id=self.summary.participantId,
            questionnaire_type='emorecog',
            etm_questionnaire_id=q_emorecog.etm_questionnaire_id,
            authored=datetime(2023, 5, 15)
        )
        flanker_rsp = self.data_generator.create_etm_questionnaire_response(
            participant_id=self.summary.participantId,
            questionnaire_type='flanker',
            etm_questionnaire_id=q_flanker.etm_questionnaire_id,
            authored=datetime(2023, 6, 30)
        )
        self.assertTrue(None not in [emorecog_rsp, flanker_rsp])
        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(flanker_rsp.authored, retention_parameters.latest_etm_response_timestamp)

    def test_etm_prt_excluded(self):
        # DA-3705: Ignore prt response data for retention since it exists unexpectedly in RDR production
        self._setup_consent_questionnaire_response(self.summary.participantId, self.etm_consent, self.etm_yes,
                                                   authored=datetime(2023, 1, 1))
        q_prt = self.data_generator.create_database_etm_questionnaire(questionnaire_type='prt')
        prt_rsp = self.data_generator.create_etm_questionnaire_response(
            participant_id=self.summary.participantId,
            questionnaire_type='prt',
            etm_questionnaire_id=q_prt.etm_questionnaire_id,
            authored=datetime(2023, 6, 30)
        )
        self.assertEqual(prt_rsp.authored, datetime(2023, 6, 30))
        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(None,  retention_parameters.latest_etm_response_timestamp)

    def _get_retention_dependencies_found(self, participant=None) -> RetentionEligibilityDependencies:
        """
        Call the code responsible for collecting the retention calculation data.
        Return the data in provided to the calculation code.
        """
        pid = participant.participantId if participant else self.summary.participantId
        _supplement_with_rdr_calculations(
            RetentionEligibleMetrics(participantId=pid),
            session=self.session
        )
        return self.retention_calc_mock.call_args[0][0]
