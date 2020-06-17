from collections import namedtuple
from datetime import datetime
import json
import mock

from rdr_service.tools.tool_libs.cope_answers import CopeAnswersClass
from tests.helpers.unittest_base import BaseTestCase

FakeFile = namedtuple('FakeFile', ['name', 'updated'])


class CopeAnswerTest(BaseTestCase):
    def setUp(self):
        super().setUp()

        # June COPE survey
        self.questionnaire_history = self.create_database_questionnaire_history(
            lastModified=datetime.strptime('2020-06-04', '%Y-%m-%d')
        )

        self.questions = {}
        for code in ['ipaq_1', 'ipaq_3', 'ipaq_5', 'ipaq_7']:
            question_code = self.create_database_code(value=code)
            self.questions[code] = self.create_database_questionnaire_question(codeId=question_code.codeId)

        self.answer_codes = {}
        for code in ['a1', 'a2', 'a3']:
            self.answer_codes[code] = self.create_database_code(value=code)

    def create_response(self, questionnaire_history, answers):
        participant = self.create_database_participant()

        questionnaire_response = self.create_database_questionnaire_response(
            participantId=participant.participantId,
            questionnaireId=questionnaire_history.questionnaireId,
            questionnaireVersion=questionnaire_history.version,
        )

        for answer in answers:
            self.create_database_questionnaire_response_answer(
                questionnaireResponseId=questionnaire_response.questionnaireResponseId,
                questionId=self.questions[answer['question']].questionnaireQuestionId,
                valueCodeId=self.answer_codes[answer['answer']].codeId
            )

    @staticmethod
    def run_tool(cope_month='2020-06'):
        environment = mock.MagicMock()
        environment.project = 'unit_test'

        args = mock.MagicMock(spec=['cope_month'])
        args.cope_month = cope_month

        # Patching things to keep tool from trying to call GAE and to get result data
        with mock.patch('rdr_service.tools.tool_libs.cope_answers.open') as mock_open:

            cope_answer_tool = CopeAnswersClass(args, environment)
            cope_answer_tool.run()

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
        result = self.run_tool()
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
        may_questionnaire_history = self.create_database_questionnaire_history(
            lastModified=datetime.strptime('2020-05-18', '%Y-%m-%d')
        )
        self.create_response(may_questionnaire_history, [
            {'question': 'ipaq_1', 'answer': 'a1'},
            {'question': 'ipaq_3', 'answer': 'a3'}
        ])

        result = self.run_tool()
        self.assertEqual({
            'ipaq_1': {'total_answers': 2,
                       'answers': {'a1': {'total_answers': 2}}},
            'ipaq_3': {'total_answers': 2,
                       'answers': {'a1': {'total_answers': 1},
                                   'a2': {'total_answers': 1}}}
        }, result)
