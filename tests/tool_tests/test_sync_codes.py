import json
import mock
import os

import rdr_service
from rdr_service.model.code import Code, CodeType
from rdr_service.tools.tool_libs.sync_codes import SyncCodesClass, REDCAP_PROJECT_KEYS
from tests.helpers.unittest_base import BaseTestCase

PROJECT_ROOT = os.path.dirname(os.path.dirname(rdr_service.__file__))


class CopeAnswerTest(BaseTestCase):
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

    def run_tool(self, redcap_data_dictionary):
        def get_server_config(*_):
            config = {
                REDCAP_PROJECT_KEYS: {
                    'project_one': '1234ABC'
                }
            }
            return json.dumps(config), 'test-file-name'

        gcp_env = mock.MagicMock()
        gcp_env.project = 'unit_test'
        gcp_env.git_project = PROJECT_ROOT
        gcp_env.get_latest_config_from_bucket = get_server_config

        args = mock.MagicMock()
        args.redcap_project = 'project_one'

        with mock.patch('rdr_service.tools.tool_libs.sync_codes.requests') as mock_requests:
            mock_response = mock_requests.post.return_value
            mock_response.status_code = 200
            mock_response.content = redcap_data_dictionary

            sync_codes_tool = SyncCodesClass(args, gcp_env)
            sync_codes_tool.run()

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
        self.assertEqual(6, self.session.query(Code).count(), "Only 6 codes should have been created")

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
        self.assertEqual(2, self.session.query(Code).count(), "Only 2 codes should have been created")

        module_code = self.assertCodeExists('TestQuestionnaire', 'Test Questionnaire Module', CodeType.MODULE)
        self.assertCodeExists('participant_id', 'Participant ID', CodeType.QUESTION, module_code)

