from dataclasses import dataclass, field
from datetime import datetime
import mock
from typing import Dict, List

from rdr_service.code_constants import PMI_SKIP_CODE
from rdr_service.dao.questionnaire_response_dao import ResponseValidator
from rdr_service.model.code import Code
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.model.survey import SurveyQuestionType
from tests.helpers.unittest_base import BaseTestCase


@dataclass
class QuestionDefinition:
    question_type: SurveyQuestionType = None
    options: List[Code] = field(default_factory=list)
    validation: str = None
    validation_min: str = None
    validation_max: str = None


@mock.patch('rdr_service.dao.questionnaire_response_dao.logging')
class ResponseValidatorTest(BaseTestCase):
    def setUp(self, **kwargs) -> None:
        super(ResponseValidatorTest, self).setUp(**kwargs)
        self.skip_answer_code = self.data_generator.create_database_code(value=PMI_SKIP_CODE)

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
                questionType=definition.question_type,
                validation=definition.validation,
                validation_min=definition.validation_min,
                validation_max=definition.validation_max
            ))
        self.data_generator.create_database_survey(
            importTime=survey_import_time,
            code=module_code,
            questions=survey_questions
        )

        # Build related QuestionnaireHistory for the response
        questionnaire_questions = [
            self.data_generator._questionnaire_question(
                codeId=question_code.codeId,
                code=question_code
            )
            for question_code in questions.keys()
        ]
        questionnaire_history = self.data_generator.create_database_questionnaire_history(
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

    def test_option_select_data_type_validation(self, mock_logging):
        """
        Survey questions that are defined with options should be answered with valueCodeId
        """
        dropdown_question_code = self.data_generator.create_database_code(value='dropdown_select')
        radio_question_code = self.data_generator.create_database_code(value='radio_select')
        checkbox_question_code = self.data_generator.create_database_code(value='checkbox_select')

        # The validator only checks to see if there are options and doesn't really mind what they are,
        # using the same options for all the questions for simplicity
        options = [
            self.data_generator.create_database_code(value=option_value)
            for option_value in ['option_a', 'option_b']
        ]

        questionnaire_history, response = self._build_questionnaire_and_response(
            questions={
                dropdown_question_code: QuestionDefinition(question_type=SurveyQuestionType.DROPDOWN, options=options),
                radio_question_code: QuestionDefinition(question_type=SurveyQuestionType.RADIO, options=options),
                checkbox_question_code: QuestionDefinition(question_type=SurveyQuestionType.CHECKBOX, options=options)
            },
            answers={
                dropdown_question_code: QuestionnaireResponseAnswer(valueString='text answer'),
                radio_question_code: QuestionnaireResponseAnswer(valueString='text answer'),
                checkbox_question_code: QuestionnaireResponseAnswer(valueString='text answer')
            }
        )

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(response)

        no_value_code_id_used_message = 'Answer for {} gives no value code id when the question has options defined'
        mock_logging.warning.assert_has_calls([
            mock.call(no_value_code_id_used_message.format(dropdown_question_code.value)),
            mock.call(no_value_code_id_used_message.format(radio_question_code.value)),
            mock.call(no_value_code_id_used_message.format(checkbox_question_code.value))
        ])

    def test_log_for_unimplemented_validation(self, mock_logging):
        calc_question_code = self.data_generator.create_database_code(value='calc_question')
        yesno_question_code = self.data_generator.create_database_code(value='yesno_question')
        truefalse_question_code = self.data_generator.create_database_code(value='truefalse_question')
        file_question_code = self.data_generator.create_database_code(value='file_question')
        slider_question_code = self.data_generator.create_database_code(value='slider_question')

        questionnaire_history, response = self._build_questionnaire_and_response(
            questions={
                calc_question_code: QuestionDefinition(question_type=SurveyQuestionType.CALC),
                yesno_question_code: QuestionDefinition(question_type=SurveyQuestionType.YESNO),
                truefalse_question_code: QuestionDefinition(question_type=SurveyQuestionType.TRUEFALSE),
                file_question_code: QuestionDefinition(question_type=SurveyQuestionType.FILE),
                slider_question_code: QuestionDefinition(question_type=SurveyQuestionType.SLIDER)
            },
            answers={
                question_code: QuestionnaireResponseAnswer()
                for question_code in [
                    calc_question_code,
                    yesno_question_code,
                    truefalse_question_code,
                    file_question_code,
                    slider_question_code
                ]
            }
        )

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(response)

        no_validation_check_message = 'No validation check implemented for answer to {} with question type {}'
        mock_logging.warning.assert_has_calls([
            mock.call(no_validation_check_message.format(calc_question_code.value, SurveyQuestionType.CALC)),
            mock.call(no_validation_check_message.format(yesno_question_code.value, SurveyQuestionType.YESNO)),
            mock.call(no_validation_check_message.format(truefalse_question_code.value, SurveyQuestionType.TRUEFALSE)),
            mock.call(no_validation_check_message.format(file_question_code.value, SurveyQuestionType.FILE)),
            mock.call(no_validation_check_message.format(slider_question_code.value, SurveyQuestionType.SLIDER))
        ])

    def test_log_for_text_questions_not_answered_with_text(self, mock_logging):
        text_question_code = self.data_generator.create_database_code(value='text_question')
        note_question_code = self.data_generator.create_database_code(value='note_question')

        questionnaire_history, response = self._build_questionnaire_and_response(
            questions={
                text_question_code: QuestionDefinition(question_type=SurveyQuestionType.TEXT),
                note_question_code: QuestionDefinition(question_type=SurveyQuestionType.NOTES)
            },
            answers={
                text_question_code: QuestionnaireResponseAnswer(valueInteger=1),
                note_question_code: QuestionnaireResponseAnswer(valueInteger=1)
            }
        )

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(response)

        mock_logging.warning.assert_has_calls([
            mock.call(f'No valueString answer given for text-based question {text_question_code.value}'),
            mock.call(f'No valueString answer given for text-based question {note_question_code.value}')
        ])

    def test_question_validation_data_type(self, mock_logging):
        """Validation strings give that a TEXT question should be another datatype"""
        date_question_code = self.data_generator.create_database_code(value='date_question')
        integer_question_code = self.data_generator.create_database_code(value='integer_question')
        unknown_question_code = self.data_generator.create_database_code(value='unknown_question')

        questionnaire_history, response = self._build_questionnaire_and_response(
            questions={
                date_question_code: QuestionDefinition(
                    question_type=SurveyQuestionType.TEXT, validation='date_mdy'
                ),
                integer_question_code: QuestionDefinition(
                    question_type=SurveyQuestionType.TEXT, validation='integer'
                ),
                unknown_question_code: QuestionDefinition(
                    question_type=SurveyQuestionType.TEXT, validation='abc'
                ),
            },
            answers={
                date_question_code: QuestionnaireResponseAnswer(valueString='test'),
                integer_question_code: QuestionnaireResponseAnswer(valueString='test'),
                unknown_question_code: QuestionnaireResponseAnswer(valueString='test')
            }
        )

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(response)

        mock_logging.warning.assert_has_calls([
            mock.call(f'No valueDate answer given for date-based question {date_question_code.value}'),
            mock.call(f'No valueInteger answer given for integer-based question {integer_question_code.value}'),
            mock.call(f'Unrecognized validation string "abc" for question {unknown_question_code.value}')
        ])

    def test_question_validation_min_max(self, mock_logging):
        date_question_code = self.data_generator.create_database_code(value='date_question')
        integer_question_code = self.data_generator.create_database_code(value='integer_question')
        broken_date_question_code = self.data_generator.create_database_code(value='broken_date_question')
        broken_integer_question_code = self.data_generator.create_database_code(value='broken_integer_question')

        questionnaire_history, response = self._build_questionnaire_and_response(
            questions={
                date_question_code: QuestionDefinition(
                    question_type=SurveyQuestionType.TEXT, validation='date_mdy', validation_min='2020-09-01'
                ),
                integer_question_code: QuestionDefinition(
                    question_type=SurveyQuestionType.TEXT, validation='integer', validation_min='0', validation_max='10'
                ),
                broken_date_question_code: QuestionDefinition(
                    question_type=SurveyQuestionType.TEXT, validation='date', validation_min='test_bad_date'
                ),
                broken_integer_question_code: QuestionDefinition(
                    question_type=SurveyQuestionType.TEXT, validation='integer', validation_min='five'
                )
            },
            answers={
                date_question_code: QuestionnaireResponseAnswer(valueDate=datetime(2020, 7, 4)),
                integer_question_code: QuestionnaireResponseAnswer(valueInteger=11),
                broken_date_question_code: QuestionnaireResponseAnswer(valueDate=datetime(2020, 7, 4)),
                broken_integer_question_code: QuestionnaireResponseAnswer(valueInteger=11),
            }
        )

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(response)

        mock_logging.warning.assert_has_calls([
            mock.call(
                f'Given answer "2020-07-04 00:00:00" is less than expected min '
                f'"2020-09-01" for question {date_question_code.value}'
            ),
            mock.call(
                f'Given answer "11" is greater than expected max "10" for question {integer_question_code.value}'
            )
        ])
        mock_logging.error.assert_has_calls([
            mock.call(
                f'Unable to parse validation string for question {broken_date_question_code.value}', exc_info=True
            ),
            mock.call(
                f'Unable to parse validation string for question {broken_integer_question_code.value}', exc_info=True
            )
        ])

    def test_option_select_option_validation(self, mock_logging):
        """
        Survey questions that are defined with options should be answered with one of those options
        """
        dropdown_question_code = self.data_generator.create_database_code(value='dropdown_select')
        unrecognized_answer_code = self.data_generator.create_database_code(value='completely_different_option')
        questionnaire_history, response = self._build_questionnaire_and_response(
            questions={
                dropdown_question_code: QuestionDefinition(question_type=SurveyQuestionType.DROPDOWN, options=[
                    self.data_generator.create_database_code(value='option_a'),
                    self.data_generator.create_database_code(value='option_b')
                ])
            },
            answers={
                dropdown_question_code: QuestionnaireResponseAnswer(
                    valueCodeId=unrecognized_answer_code.codeId
                )
            }
        )

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(response)

        mock_logging.warning.assert_called_with(
            f'Code ID {unrecognized_answer_code.codeId} is an invalid answer to {dropdown_question_code.value}'
        )

    def test_any_question_can_be_skipped(self, mock_logging):
        """
        Any question should be able to be skipped, even if it doesn't take option codes
        """
        select_code = self.data_generator.create_database_code(value='select')
        text_question_code = self.data_generator.create_database_code(value='text')
        questionnaire_history, response = self._build_questionnaire_and_response(
            questions={
                select_code: QuestionDefinition(question_type=SurveyQuestionType.DROPDOWN, options=[
                    self.data_generator.create_database_code(value='option_a'),
                    self.data_generator.create_database_code(value='option_b')
                ]),
                text_question_code: QuestionDefinition(question_type=SurveyQuestionType.TEXT)
            },
            answers={
                select_code: QuestionnaireResponseAnswer(valueCodeId=self.skip_answer_code.codeId),
                text_question_code: QuestionnaireResponseAnswer(valueCodeId=self.skip_answer_code.codeId)
            }
        )

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(response)

        # No logs should have been made because of the skip codes
        mock_logging.warning.assert_not_called()
        mock_logging.error.assert_not_called()

    def test_questions_answered_multiple_times(self, mock_logging):
        """We should only get one answer for a question (except Checkbox questions)"""
        dropdown_question_code = self.data_generator.create_database_code(value='dropdown_select')
        checkbox_question_code = self.data_generator.create_database_code(value='checkbox_select')

        # The validator only checks to see if there are options and doesn't really mind what they are,
        # using the same options for all the questions for simplicity
        option_a_code = self.data_generator.create_database_code(value='option_a')
        option_b_code = self.data_generator.create_database_code(value='option_b')
        options = [option_a_code, option_b_code]

        questionnaire_history, response = self._build_questionnaire_and_response(
            questions={
                dropdown_question_code: QuestionDefinition(question_type=SurveyQuestionType.DROPDOWN, options=options),
                checkbox_question_code: QuestionDefinition(question_type=SurveyQuestionType.CHECKBOX, options=options)
            },
            answers={
                dropdown_question_code: QuestionnaireResponseAnswer(valueCodeId=option_a_code.codeId),
                checkbox_question_code: QuestionnaireResponseAnswer(valueCodeId=option_a_code.codeId)
            }
        )
        # Add extra answers to the response for each question
        for question in questionnaire_history.questions:
            response.answers.append(QuestionnaireResponseAnswer(
                questionId=question.questionnaireQuestionId,
                valueCodeId=option_b_code.codeId
            ))

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(response)

        mock_logging.error.assert_called_once_with(f'Too many answers given for {dropdown_question_code.value}')

    def test_unknown_types_can_be_checkboxes(self, mock_logging):
        """
        We should assume the best and allow for unknown types to be answered multiple times (they could be checkboxes)
        """
        multi_select = self.data_generator.create_database_code(value='dropdown_select')

        # The validator only checks to see if there are options and doesn't really mind what they are,
        # using the same options for all the questions for simplicity
        option_a_code = self.data_generator.create_database_code(value='option_a')
        option_b_code = self.data_generator.create_database_code(value='option_b')
        options = [option_a_code, option_b_code]

        questionnaire_history, response = self._build_questionnaire_and_response(
            questions={
                multi_select: QuestionDefinition(question_type=SurveyQuestionType.UNKNOWN, options=options)
            },
            answers={
                multi_select: QuestionnaireResponseAnswer(valueCodeId=option_a_code.codeId)
            }
        )
        # Add extra answers to the response for each question
        for question in questionnaire_history.questions:
            response.answers.append(QuestionnaireResponseAnswer(
                questionId=question.questionnaireQuestionId,
                valueCodeId=option_b_code.codeId
            ))

        validator = ResponseValidator(questionnaire_history, self.session)
        validator.check_response(response)

        # No logs should have been made because of the additional answers
        mock_logging.warning.assert_not_called()
        mock_logging.error.assert_not_called()
