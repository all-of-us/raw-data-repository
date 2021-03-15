from dataclasses import dataclass, field
from datetime import datetime
import mock
from typing import Dict, List

from rdr_service.dao.questionnaire_response_dao import ResponseValidator
from rdr_service.model.code import Code
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireHistory
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireQuestion,\
    QuestionnaireResponseAnswer
from rdr_service.model.survey import SurveyQuestionType
from tests.helpers.unittest_base import BaseTestCase


@dataclass
class QuestionDefinition:
    question_type: SurveyQuestionType = None
    options: List[Code] = field(default_factory=list)


class ResponseValidatorTest(BaseTestCase):
    def _build_questionnaire_and_response(self, questions: Dict[Code, QuestionDefinition],
                                          answers: Dict[Code, QuestionnaireResponseAnswer],
                                          survey_import_time=datetime(2020, 12, 4),
                                          questionnaire_created_time=datetime(2021, 4, 1)):
        module_code = self.data_generator.create_database_code(value='test_survey')

        # Build survey structure for defined questions
        survey_questions = []
        for question_code, definition in questions.items():
            survey_question_options = [
                self.data_generator.create_database_survey_question_option(codeId=option_code.codeId)
                for option_code in definition.options
            ]
            survey_questions.append(self.data_generator.create_database_survey_question(
                code=question_code,
                options=survey_question_options,
                questionType=definition.question_type
            ))
        self.data_generator.create_database_survey(
            importTime=survey_import_time,
            code=module_code,
            questions=survey_questions
        )

        # Build related QuestionnaireHistory for the response
        questionnaire_questions = [
            QuestionnaireQuestion(
                codeId=question_code.codeId,
                code=question_code
            )
            for question_code in questions.keys()
        ]
        questionnaire_history = QuestionnaireHistory(
            questions=questionnaire_questions,
            concepts=[QuestionnaireConcept(codeId=module_code.codeId)],
            created=questionnaire_created_time
        )

        # Build the response to the questionnaire
        question_code_map: Dict[int, QuestionnaireQuestion] = {
            question.codeId: question for question in questionnaire_questions
        }
        for code, answer in answers.items():
            question = question_code_map[code.codeId]
            answer.questionId = question.questionnaireQuestionId
            answer.question = question
        questionnaire_response = QuestionnaireResponse(
            answers=list(answers.values())
        )

        return questionnaire_history, questionnaire_response

    @mock.patch('rdr_service.dao.questionnaire_response_dao.logging')
    def test_simple_survey_response_validation(self, mock_logging):
        """
        For surveys derived from the legacy code system, we can only verify that questions that
        have options are responded to with valueCodeId
        """
        multi_select_question_code = self.data_generator.create_database_code(value='multi_select')
        free_text_question_code = self.data_generator.create_database_code(value='free_text')

        questionnaire_history, response = self._build_questionnaire_and_response(
            questions={
                multi_select_question_code: QuestionDefinition(question_type=SurveyQuestionType.UNKNOWN, options=[
                    self.data_generator.create_database_code(value='option_a'),
                    self.data_generator.create_database_code(value='option_b')
                ]),
                free_text_question_code: QuestionDefinition(question_type=SurveyQuestionType.UNKNOWN)
            },
            answers={
                multi_select_question_code: QuestionnaireResponseAnswer(
                    valueString='answering with string rather than something selected from a list of options'
                ),
                free_text_question_code: QuestionnaireResponseAnswer(
                    valueCodeId=self.data_generator.create_database_code(value='unknown_option')
                )
            }
        )

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(response)

        mock_logging.warning.assert_has_calls([
            mock.call(f'Answer for {multi_select_question_code.value} gives no value code id when the question '
                      f'has options defined'),
            mock.call(f'Answer for {free_text_question_code.value} gives a value code id when no options are defined')
        ])

