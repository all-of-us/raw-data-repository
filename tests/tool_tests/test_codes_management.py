import json
import mock
import os

import rdr_service
from rdr_service.model.code import Code, CodeType
from rdr_service.tools.tool_libs.codes_management import CodesManagementClass, REDCAP_PROJECT_KEYS
from tests.helpers.unittest_base import BaseTestCase

PROJECT_ROOT = os.path.dirname(os.path.dirname(rdr_service.__file__))


class CodesManagementTest(BaseTestCase):

    @staticmethod
    def _get_mock_dictionary_item(code_value, description, field_type, answers=''):
        return {
            "field_name": code_value,
            "form_name": "survey",
            "section_header": "",
            "field_type": field_type,
            "field_label": description,
            "select_choices_or_calculations": answers,
            "field_note": "",
            "text_validation_type_or_show_slider_number": "",
            "text_validation_min": "",
            "text_validation_max": "",
            "identifier": "",
            "branching_logic": "",
            "required_field": "",
            "custom_alignment": "",
            "question_number": "",
            "matrix_group_name": "",
            "matrix_ranking": "",
            "field_annotation": ""
        }

    @staticmethod
    def run_tool(redcap_data_dictionary, reuse_codes=None):
        def get_server_config(*_):
            config = {
                REDCAP_PROJECT_KEYS: {
                    'project_one': '1234ABC'
                }
            }
            return json.dumps(config), 'test-file-name'

        gcp_env = mock.MagicMock()
        gcp_env.project = 'localhost'
        gcp_env.git_project = PROJECT_ROOT
        gcp_env.get_latest_config_from_bucket = get_server_config

        args = mock.MagicMock()
        args.redcap_project = 'project_one'
        if reuse_codes:
            args.reuse_codes = ','.join(reuse_codes)

        with mock.patch('rdr_service.tools.tool_libs.codes_management.requests') as mock_requests:
            mock_response = mock_requests.post.return_value
            mock_response.status_code = 200
            mock_response.content = redcap_data_dictionary

            sync_codes_tool = CodesManagementClass(args, gcp_env)
            return sync_codes_tool.run()

    def _load_code_with_value(self, code_value) -> Code:
        return self.session.query(Code).filter(Code.value == code_value).one()

    def assertCodeExists(self, code_value, display_text, code_type, parent_code: Code = None):
        code = self._load_code_with_value(code_value)
        self.assertEqual(display_text, code.display)
        self.assertEqual(code_type, code.codeType)

        if parent_code:
            self.assertEqual(parent_code.codeId, code.parentId)
        else:
            self.assertIsNone(code.parentId)

        return code

    def test_question_and_answer_codes(self):
        self.run_tool([
            self._get_mock_dictionary_item(
                'participant_id',
                'Participant ID',
                'text'
            ),
            self._get_mock_dictionary_item(
                'radio',
                'This is a single-select, multiple choice question',
                'radio',
                answers='A1, Choice One | A2, Choice Two | A3, Choice Three | A4, Etc.'
            )
        ])
        self.assertEqual(6, self.session.query(Code).count(), "6 codes should have been created")

        self.assertCodeExists('participant_id', 'Participant ID', CodeType.QUESTION)
        radio_code = self.assertCodeExists(
            'radio',
            'This is a single-select, multiple choice question',
            CodeType.QUESTION
        )
        self.assertCodeExists('A1', 'Choice One', CodeType.ANSWER, radio_code)
        self.assertCodeExists('A2', 'Choice Two', CodeType.ANSWER, radio_code)
        self.assertCodeExists('A3', 'Choice Three', CodeType.ANSWER, radio_code)
        self.assertCodeExists('A4', 'Etc.', CodeType.ANSWER, radio_code)

    def test_detection_of_module_code(self):
        self.run_tool([
            self._get_mock_dictionary_item(
                'TestQuestionnaire',
                'Test Questionnaire Module',
                'descriptive'
            ),
            self._get_mock_dictionary_item(
                'participant_id',
                'Participant ID',
                'text'
            ),
            self._get_mock_dictionary_item(
                'another_descriptive',
                'This is another readonly section of the questionnaire',
                'descriptive'
            )
        ])
        self.assertEqual(2, self.session.query(Code).count(), "2 codes should have been created")

        module_code = self.assertCodeExists('TestQuestionnaire', 'Test Questionnaire Module', CodeType.MODULE)
        self.assertCodeExists('participant_id', 'Participant ID', CodeType.QUESTION, module_code)

    @mock.patch('rdr_service.tools.tool_libs.codes_management.logger')
    def test_failure_on_question_code_reuse(self, mock_logger):
        self.data_generator.create_database_code(value='old_code')

        return_val = self.run_tool([
            self._get_mock_dictionary_item(
                'TestQuestionnaire',
                'Test Questionnaire Module',
                'descriptive'
            ),
            self._get_mock_dictionary_item(
                'participant_id',
                'Participant ID',
                'text'
            ),
            self._get_mock_dictionary_item(
                'old_code',
                'This is unintentional re-use',
                'text'
            ),
            self._get_mock_dictionary_item(
                'another_code',
                'Just making sure other codes do not get saved',
                'text'
            )
        ])
        self.assertEqual(1, self.session.query(Code).count(), 'No codes should be created when running the tool')
        mock_logger.error.assert_any_call('Code "old_code" is already in use')
        self.assertEqual(1, return_val, 'Script should exit with an error code')

    def test_auto_ignore_answer_code_reuse(self):
        self.data_generator.create_database_code(value='A1')

        return_val = self.run_tool([
            self._get_mock_dictionary_item(
                'radio',
                'This is a single-select, multiple choice question',
                'radio',
                answers='A1, Choice One | A2, Choice Two | A3, Choice Three | A4, Etc.'
            )
        ])
        self.assertEqual(5, self.session.query(Code).count(), 'Should be 5 codes after test')
        self.assertEqual(0, return_val, 'Script should successfully exit, ignoring that the answer code was reused')

    def test_allowing_for_explicit_question_code_reuse(self):
        self.data_generator.create_database_code(value='TestQuestionnaire')
        self.data_generator.create_database_code(value='old_code')

        return_val = self.run_tool([
            self._get_mock_dictionary_item(
                'TestQuestionnaire',
                'Test Questionnaire Module',
                'descriptive'
            ),
            self._get_mock_dictionary_item(
                'participant_id',
                'Participant ID',
                'text'
            ),
            self._get_mock_dictionary_item(
                'old_code',
                'This is unintentional re-use',
                'text'
            ),
            self._get_mock_dictionary_item(
                'another_code',
                'Just making sure other codes do not get saved',
                'text'
            )
        ], reuse_codes=['old_code', 'TestQuestionnaire'])
        self.assertEqual(4, self.session.query(Code).count(),
                         'Only 2 new codes should be created (with 2 previously existing)')
        self.assertEqual(0, return_val,
                         'Script should successfully exit, allowing for intentional reuse of the question code')

