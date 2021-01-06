from rdr_service.model.code import CodeType
from tests.helpers.unittest_base import BaseTestCase


class CheckPpiDataApiTest(BaseTestCase):
    def setUp(self):
        super(CheckPpiDataApiTest, self).setUp(with_consent_codes=True)

        self.participant_summary = self.data_generator.create_database_participant_summary(email='test@example.com')

        questions_and_answers = [
            ('first_question_code', 'first_answer_code'),
            ('Second_CODE', 'ANOTHER_ANSWER'),
            ('LAST_CODE', 'Final_Answer|with_additional_option')
        ]

        questionnaire = self.data_generator.create_database_questionnaire_history()
        for question_code_value, _ in questions_and_answers:
            question_code = self.data_generator.create_database_code(
                value=question_code_value,
                codeType=CodeType.QUESTION
            )
            self.data_generator.create_database_questionnaire_question(
                questionnaireId=questionnaire.questionnaireId,
                questionnaireVersion=questionnaire.version,
                codeId=question_code.codeId
            )

        questionnaire_response = self.data_generator.create_database_questionnaire_response(
            participantId=self.participant_summary.participantId,
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version
        )
        for question_index, (_, answer_code_values) in enumerate(questions_and_answers):
            question = questionnaire.questions[question_index]

            for answer_value in answer_code_values.split('|'):
                answer_code = self.data_generator.create_database_code(value=answer_value)
                self.data_generator.create_database_questionnaire_response_answer(
                    questionnaireResponseId=questionnaire_response.questionnaireResponseId,
                    questionId=question.questionnaireQuestionId,
                    valueCodeId=answer_code.codeId
                )

    def test_case_insensitive_answer_code_matching(self):
        """Make sure case doesn't matter when matching answer codes against what the server has"""

        ppi_check_payload = {
            'ppi_data': {
                self.participant_summary.email: {
                    'fIrSt_QuEsTiOn_CoDe': 'First_Answer_Code',
                    'SECOND_CODE': 'another_answer',
                    'last_code': 'Final_ANSWER|WITH_ADDITIONAL_OPTION'
                }
            }
        }
        response = self.send_post('CheckPpiData', ppi_check_payload)

        response_error_count = response['ppi_results']['test@example.com']['errors_count']
        self.assertEqual(0, response_error_count, 'Differences in case should not cause errors')
