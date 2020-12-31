from datetime import datetime
import mock

from rdr_service.code_constants import CONSENT_FOR_STUDY_ENROLLMENT_MODULE
from rdr_service.etl.model.src_clean import SrcClean
from rdr_service.model.participant import Participant
from rdr_service.tools.tool_libs.curation import CurationExportClass
from tests.helpers.unittest_base import BaseTestCase


class CurationEtlTest(BaseTestCase):
    def setUp(self):
        super(CurationEtlTest, self).setUp()
        self._setup_data()

    def _setup_data(self):
        self.participant = self.data_generator.create_database_participant()

        module_code = self.data_generator.create_database_code(value='src_clean_test')

        self.questionnaire = self.data_generator.create_database_questionnaire_history()
        for question_index in range(4):
            question_code = self.data_generator.create_database_code(value=f'q_{question_index}')
            self.data_generator.create_database_questionnaire_question(
                questionnaireId=self.questionnaire.questionnaireId,
                questionnaireVersion=self.questionnaire.version,
                codeId=question_code.codeId
            )

        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=self.questionnaire.questionnaireId,
            questionnaireVersion=self.questionnaire.version,
            codeId=module_code.codeId
        )

        self.questionnaire_response = self._setup_questionnaire_response(self.participant, self.questionnaire)

    def _setup_questionnaire_response(self, participant, questionnaire, authored=datetime(2020, 3, 15),
                                     created=datetime(2020, 3, 15), indexed_answers=None):
        questionnaire_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            authored=authored,
            created=created
        )

        if indexed_answers is None:
            # If no answers were specified then answer all questions with 'test answer'
            indexed_answers = [
                (question_index, 'test answer')
                for question_index in range(len(questionnaire.questions))
            ]

        for question_index, answer_string in indexed_answers:
            question = questionnaire.questions[question_index]
            self.data_generator.create_database_questionnaire_response_answer(
                questionnaireResponseId=questionnaire_response.questionnaireResponseId,
                questionId=question.questionnaireQuestionId,
                valueString=answer_string
            )

        return questionnaire_response

    @staticmethod
    def run_tool():
        gcp_env = mock.MagicMock()

        args = mock.MagicMock()
        args.command = 'cdm-data'

        cope_answer_tool = CurationExportClass(args, gcp_env)
        cope_answer_tool.run()

    def test_locking(self):
        """Make sure that building the CDM tables doesn't take exclusive locks"""

        # Take an exclusive lock on the participant, one of the records known to be part of the insert query
        self.session.query(Participant).filter(
            Participant.participantId == self.participant.participantId
        ).with_for_update().one()

        # This will time out if the tool tries to take an exclusive lock on the participant
        self.run_tool()

    def _src_clean_record_found_for_response(self, questionnaire_response_id):
        response_record = self.session.query(SrcClean).filter(
            SrcClean.questionnaire_response_id == questionnaire_response_id
        ).one_or_none()
        return response_record is not None

    def test_latest_questionnaire_response_used(self):
        """The latest questionnaire response received for a module should be used"""
        # Note: this only applies to modules that shouldn't roll up answers (ConsentPII should be rolled up)

        # Create a questionnaire response that would be used instead of the default for the test suite
        self._setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            indexed_answers=[
                (1, 'update'),
                (3, 'final answer')
            ],
            authored=datetime(2020, 5, 10),
            created=datetime(2020, 5, 10)
        )

        # Check that we are only be seeing the answers from the latest questionnaire response
        self.run_tool()
        for question_index, question in enumerate(self.questionnaire.questions):
            expected_answer = None
            if question_index == 1:
                expected_answer = 'update'
            elif question_index == 3:
                expected_answer = 'final answer'

            src_clean_answer = self.session.query(SrcClean).filter(
                SrcClean.question_code_id == question.codeId
            ).one_or_none()
            if expected_answer is None:
                self.assertIsNone(src_clean_answer)
            else:
                self.assertEqual(expected_answer, src_clean_answer.value_string)

    def _create_consent_questionnaire(self):
        module_code = self.data_generator.create_database_code(value=CONSENT_FOR_STUDY_ENROLLMENT_MODULE)
        consent_question_codes = [
            self.data_generator.create_database_code(value=f'consent_q_code_{question_index}')
            for question_index in range(4)
        ]

        consent_questionnaire = self.data_generator.create_database_questionnaire_history()
        for consent_question_code in consent_question_codes:
            self.data_generator.create_database_questionnaire_question(
                questionnaireId=consent_questionnaire.questionnaireId,
                questionnaireVersion=consent_questionnaire.version,
                codeId=consent_question_code.codeId
            )

        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=consent_questionnaire.questionnaireId,
            questionnaireVersion=consent_questionnaire.version,
            codeId=module_code.codeId
        )

        return consent_questionnaire

    def test_consent_response_answers_roll_up(self):
        """
        For the consent survey, all of the most recent answers for a code should be used
        even if they were in previous responses.
        """

        consent_questionnaire = self._create_consent_questionnaire()

        self._setup_questionnaire_response(self.participant, consent_questionnaire)
        self._setup_questionnaire_response(
            self.participant,
            consent_questionnaire,
            indexed_answers=[(1, 'NewLastName'), (3, 'new-email')],
            authored=datetime(2020, 5, 1)
        )
        self._setup_questionnaire_response(
            self.participant,
            consent_questionnaire,
            indexed_answers=[(2, 'updated address'), (3, 'corrected-email')],
            authored=datetime(2020, 8, 1)
        )

        # Check that the newest answer is in the src_clean, even if it wasn't from the latest response
        self.run_tool()
        for question_index, question in enumerate(consent_questionnaire.questions):
            expected_answer = 'test answer'
            if question_index == 1:
                expected_answer = 'NewLastName'
            elif question_index == 2:
                expected_answer = 'updated address'
            elif question_index == 3:
                expected_answer = 'corrected-email'

            # Since there was an initial response with an answer for every question, then every question
            # should have an answer in the export (even though partial responses updated some of them).
            # There also shouldn't be multiple answers from the participant for any of the survey
            # questions in the export.
            src_clean_answer_query = self.session.query(SrcClean).filter(
                SrcClean.question_code_id == question.codeId
            ).one()
            self.assertEqual(expected_answer, src_clean_answer_query.value_string)
