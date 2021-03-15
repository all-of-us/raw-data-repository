from datetime import datetime
import mock

from rdr_service.dao.questionnaire_response_dao import ResponseValidator
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireHistory
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.model.survey import SurveyQuestionType
from tests.helpers.unittest_base import BaseTestCase


class ResponseValidatorTest(BaseTestCase):
    @mock.patch('rdr_service.dao.questionnaire_response_dao.logging')
    def test_simple_survey_response_validation(self, mock_logging):
        """
        This test uses a Survey structure that looks like what we have from the legacy code system.
        """
        module_code = self.data_generator.create_database_code(value='test_survey')
        multi_select_question_code = self.data_generator.create_database_code(value='multi_select')

        survey_import_time = datetime(2020, 12, 4)
        questionnaire_created_time = datetime(2021, 4, 1)

        survey_question_options = [
            self.data_generator.create_database_survey_question_option(codeId=option_code.codeId)
            for option_code in [
                self.data_generator.create_database_code(value='option_a'),
                self.data_generator.create_database_code(value='option_b')
            ]
        ]
        survey_question = self.data_generator.create_database_survey_question(
            code=multi_select_question_code,
            options=survey_question_options,
            questionType=SurveyQuestionType.UNKNOWN
        )
        self.data_generator.create_database_survey(
            importTime=survey_import_time,
            code=module_code,
            questions=[survey_question]
        )

        questionnaire_concept = QuestionnaireConcept(codeId=module_code.codeId)
        questionnaire_history = QuestionnaireHistory(
            created=questionnaire_created_time,
            concepts=[questionnaire_concept]
        )

        multi_select_answer = QuestionnaireResponseAnswer(
            valueString='answering with string rather than something selected from a list of options'
        )
        questionnaire_response = QuestionnaireResponse(
            answers=[multi_select_answer]
        )

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(questionnaire_response)

        mock_logging.assert_called_with()

