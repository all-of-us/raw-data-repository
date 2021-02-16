
from rdr_service.model.code import CodeType
from rdr_service.model.survey import Survey, SurveyQuestionType
from rdr_service.tools.tool_libs.survey_backfill import SurveyBackFill
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.tool_test_mixin import ToolTestMixin


class CopeAnswerTest(ToolTestMixin, BaseTestCase):

    def test_creating_basic_survey_structure(self):
        module_code = self.data_generator.create_database_code(
            display='Test module',
            value='test_module',
            codeType=CodeType.MODULE
        )

        # Make a simple question in the module
        self.data_generator.create_database_code(
            display='free text question',
            value='text_question',
            codeType=CodeType.QUESTION,
            parentId=module_code.codeId
        )

        # Make a question with options in the module
        multiple_choice_code = self.data_generator.create_database_code(
            display='multiple choice question',
            value='choice_question',
            codeType=CodeType.QUESTION,
            parentId=module_code.codeId
        )
        self.data_generator.create_database_code(
            display='Option A',
            value='option_a',
            codeType=CodeType.ANSWER,
            parentId=multiple_choice_code.codeId
        )
        self.data_generator.create_database_code(
            display='Option B',
            value='option_b',
            codeType=CodeType.ANSWER,
            parentId=multiple_choice_code.codeId
        )

        # Run the back fill to create the survey objects for the codes
        self.run_tool(SurveyBackFill, tool_args={
            'module_code': 'test_module'
        })

        # Check that everything transferred as expected to the Survey structure
        survey: Survey = self.session.query(Survey).one()

        self.assertEqual('Test module', survey.redcapProjectTitle)

        first_question = survey.questions[0]
        self.assertEqual('free text question', first_question.display)
        self.assertEqual(SurveyQuestionType.UNKNOWN, first_question.questionType)

        second_question = survey.questions[1]
        self.assertEqual('multiple choice question', second_question.display)
        self.assertEqual(SurveyQuestionType.UNKNOWN, second_question.questionType)

        # Check that the options got set up
        self.assertEqual('Option A', second_question.options[0].display)
        self.assertEqual('Option B', second_question.options[1].display)

    def test_topic_codes_are_transparent(self):
        """To make sure we can get SurveyQuestions set up for questions that are nested in topics"""
        module_code = self.data_generator.create_database_code(
            display='Test module',
            value='test_module',
            codeType=CodeType.MODULE
        )

        # Make a simple question in the module
        self.data_generator.create_database_code(
            display='free text question',
            value='text_question',
            codeType=CodeType.QUESTION,
            parentId=module_code.codeId
        )

        # Make a topic
        topic_code = self.data_generator.create_database_code(
            display='Topic',
            value='topic_code',
            codeType=CodeType.TOPIC,
            parentId=module_code.codeId
        )

        # Add some questions to the topic
        self.data_generator.create_database_code(
            display='topic question one',
            value='topic_one',
            codeType=CodeType.QUESTION,
            parentId=topic_code.codeId
        )
        self.data_generator.create_database_code(
            display='topic question two',
            value='topic_two',
            codeType=CodeType.QUESTION,
            parentId=topic_code.codeId
        )

        # Run the back fill to create the survey objects for the codes
        self.run_tool(SurveyBackFill, tool_args={
            'module_code': 'test_module'
        })

        # Check that all the expected questions were created for the survey
        survey: Survey = self.session.query(Survey).one()
        self.assertEqual('free text question', survey.questions[0].display)
        self.assertEqual('topic question one', survey.questions[1].display)
        self.assertEqual('topic question two', survey.questions[2].display)

