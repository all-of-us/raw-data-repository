from collections import namedtuple
import json
import mock
import os

import rdr_service
from rdr_service import config
from rdr_service.tools.tool_libs.cope_answers import CopeAnswersClass
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.tool_test_mixin import ToolTestMixin

PROJECT_ROOT = os.path.dirname(os.path.dirname(rdr_service.__file__))

FakeFile = namedtuple('FakeFile', ['name', 'updated'])


class CopeAnswerTest(ToolTestMixin, BaseTestCase):
    def setUp(self):
        super().setUp()

        self.questionnaire_history = self.data_generator.create_database_questionnaire_history(
            externalId='june_cope'
        )

        self.questions = {}
        for code in ['ipaq_1', 'ipaq_3', 'ipaq_5', 'ipaq_7']:
            question_code = self.data_generator.create_database_code(value=code)
            self.questions[code] = self.data_generator.create_database_questionnaire_question(codeId=question_code.codeId)

        self.answer_codes = {}
        for code in ['a1', 'a2', 'a3']:
            self.answer_codes[code] = self.data_generator.create_database_code(value=code)

    def create_response(self, questionnaire_history, answers):
        participant = self.data_generator.create_database_participant()

        questionnaire_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            questionnaireId=questionnaire_history.questionnaireId,
            questionnaireVersion=questionnaire_history.version,
        )

        for answer in answers:
            self.data_generator.create_database_questionnaire_response_answer(
                questionnaireResponseId=questionnaire_response.questionnaireResponseId,
                questionId=self.questions[answer['question']].questionnaireQuestionId,
                valueCodeId=self.answer_codes[answer['answer']].codeId
            )

    @staticmethod
    def aggregate_cope_answers(cope_month='June'):
        with mock.patch('rdr_service.tools.tool_libs.cope_answers.open') as mock_open:
            CopeAnswerTest.run_tool(CopeAnswersClass, tool_args={
                'cope_month': cope_month,
                'codes': 'ipaq_1, ipaq_3, ipaq_5, ipaq_7'
            }, server_config={
                config.COPE_FORM_ID_MAP: {
                    'may_cope': 'May',
                    'june_cope': 'June'
                }
            })

            # Return data written to file as JSON
            mock_file_write = mock_open.return_value.__enter__.return_value.write
            if mock_file_write.mock_calls:  # make sure we don't fail if there weren't supposed to be calls
                file_data = mock_file_write.mock_calls[0].args[0]
                result = json.loads(file_data)
                return result

    def test_answer_counts(self):
        self.create_response(self.questionnaire_history, [
            {'question': 'ipaq_1', 'answer': 'a1'},
            {'question': 'ipaq_3', 'answer': 'a1'},
            {'question': 'ipaq_5', 'answer': 'a2'},
            {'question': 'ipaq_7', 'answer': 'a2'}
        ])
        self.create_response(self.questionnaire_history, [
            {'question': 'ipaq_1', 'answer': 'a1'},
            {'question': 'ipaq_3', 'answer': 'a2'},
            {'question': 'ipaq_5', 'answer': 'a2'},
            {'question': 'ipaq_7', 'answer': 'a2'}
        ])
        self.create_response(self.questionnaire_history, [
            {'question': 'ipaq_1', 'answer': 'a1'},
            {'question': 'ipaq_3', 'answer': 'a3'},
            {'question': 'ipaq_7', 'answer': 'a1'}
        ])
        result = self.aggregate_cope_answers()
        self.assertEqual({
            'ipaq_1': {'total_answers': 3,
                       'answers': {'a1': {'total_answers': 3}}},
            'ipaq_3': {'total_answers': 3,
                       'answers': {'a1': {'total_answers': 1},
                                   'a2': {'total_answers': 1},
                                   'a3': {'total_answers': 1}}},
            'ipaq_5': {'total_answers': 2,
                       'answers': {'a2': {'total_answers': 2}}},
            'ipaq_7': {'total_answers': 3,
                       'answers': {'a1': {'total_answers': 1},
                                   'a2': {'total_answers': 2}}}
        }, result)

    def test_count_from_specified_month(self):
        self.create_response(self.questionnaire_history, [
            {'question': 'ipaq_1', 'answer': 'a1'},
            {'question': 'ipaq_3', 'answer': 'a1'}
        ])
        self.create_response(self.questionnaire_history, [
            {'question': 'ipaq_1', 'answer': 'a1'},
            {'question': 'ipaq_3', 'answer': 'a2'},
        ])

        # Create May response that should not be counted for June results
        may_questionnaire_history = self.data_generator.create_database_questionnaire_history(
            externalId='may_cope'
        )
        self.create_response(may_questionnaire_history, [
            {'question': 'ipaq_1', 'answer': 'a1'},
            {'question': 'ipaq_3', 'answer': 'a3'}
        ])

        result = self.aggregate_cope_answers()
        self.assertEqual({
            'ipaq_1': {'total_answers': 2,
                       'answers': {'a1': {'total_answers': 2}}},
            'ipaq_3': {'total_answers': 2,
                       'answers': {'a1': {'total_answers': 1},
                                   'a2': {'total_answers': 1}}}
        }, result)
