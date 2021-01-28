from datetime import datetime
import mock

from rdr_service.code_constants import CONSENT_FOR_STUDY_ENROLLMENT_MODULE, STREET_ADDRESS_QUESTION_CODE,\
    STREET_ADDRESS2_QUESTION_CODE
from rdr_service.etl.model.src_clean import SrcClean
from rdr_service.model.code import Code
from rdr_service.model.participant import Participant
from rdr_service.participant_enums import QuestionnaireResponseStatus
from rdr_service.tools.tool_libs.curation import CurationExportClass
from tests.helpers.unittest_base import BaseTestCase


class CurationEtlTest(BaseTestCase):
    def setUp(self):
        super(CurationEtlTest, self).setUp(with_consent_codes=True)
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
                                      created=datetime(2020, 3, 15), indexed_answers=None,
                                      status=QuestionnaireResponseStatus.COMPLETED):
        questionnaire_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            authored=authored,
            created=created,
            status=status
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
        module_code = self.session.query(Code).filter(Code.value == CONSENT_FOR_STUDY_ENROLLMENT_MODULE).one()
        consent_question_codes = [
            self.data_generator.create_database_code(value=f'consent_q_code_{question_index}')
            for question_index in range(4)
        ]
        consent_question_codes += self.session.query(Code).filter(Code.value.in_([
            STREET_ADDRESS_QUESTION_CODE, STREET_ADDRESS2_QUESTION_CODE
        ])).all()

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

    def test_consent_address_roll_up(self):
        """
        For the consent survey, any answers for the first line of the street address should also
        override previous answers for StreetAddress2
        """

        consent_questionnaire = self._create_consent_questionnaire()

        # Set up a response that answers all the questions, including the two address lines
        self._setup_questionnaire_response(self.participant, consent_questionnaire)
        self._setup_questionnaire_response(
            self.participant,
            consent_questionnaire,
            authored=datetime(2020, 5, 1)
        )

        # Enter another response that just updates the first line of the street address
        expected_final_address = '42 Wallaby Way'
        self._setup_questionnaire_response(
            self.participant,
            consent_questionnaire,
            indexed_answers=[(4, expected_final_address)],  # Assuming the 4th question is the first line of the address
            authored=datetime(2020, 8, 1)
        )

        # Check that the only address answer in src_clean is the updated line 1 for the street address (which will
        # also replace line 2 for the the first response)
        self.run_tool()

        # Load all src_clean rows for lines 1 and 2 of the address
        address_answers = self.session.query(SrcClean).join(
            Code, Code.codeId == SrcClean.question_code_id
        ).filter(
            Code.value.in_([STREET_ADDRESS_QUESTION_CODE, STREET_ADDRESS2_QUESTION_CODE])
        ).all()

        # Make sure we only get the updated address and nothing from the original response
        self.assertEqual(
            1,
            len(address_answers),
            'The updated street address should overwrite the previous answers for line 1 and 2 of the address'
        )
        self.assertEqual(expected_final_address, address_answers[0].value_string)

    def test_in_progress_responses_are_filtered_out_of_export(self):
        """
        We will filter in-progress questionnaire responses out during the ETL process until the curation team is
        ready to start receiving them.
        """

        # Create an in-progress questionnaire response that should not appear in src_clean
        participant = self.data_generator.create_database_participant()
        self._setup_questionnaire_response(
            participant,
            self.questionnaire,
            status=QuestionnaireResponseStatus.IN_PROGRESS
        )
        self.run_tool()

        # Check that src_clean doesn't have any records for the in-progress questionnaire response
        src_clean_answers = self.session.query(SrcClean).filter(
            SrcClean.participant_id == participant.participantId
        ).all()
        self.assertEmpty(src_clean_answers)

    def test_later_in_progress_response_not_used(self):
        """
        Make sure later, in-progress responses don't make us filter out full and valid responses that should be used
        """

        # Create a questionnaire response that might be used instead of the default for the test suite
        in_progress_response = self._setup_questionnaire_response(
            self.participant,
            self.questionnaire,
            authored=datetime(2020, 5, 10),
            created=datetime(2020, 5, 10),
            status=QuestionnaireResponseStatus.IN_PROGRESS
        )
        self.run_tool()

        # Make sure src_clean only has data from the full response
        in_progress_answers = self.session.query(SrcClean).filter(
            SrcClean.questionnaire_response_id == in_progress_response.questionnaireResponseId
        ).all()
        self.assertEmpty(in_progress_answers)

        complete_answers = self.session.query(SrcClean).filter(
            SrcClean.questionnaire_response_id == self.questionnaire_response.questionnaireResponseId
        ).all()
        self.assertNotEmpty(complete_answers)
