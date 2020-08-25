import json
import mock
import os

import rdr_service
from rdr_service.model.code import Code
from rdr_service.tools.tool_libs.sync_codes import SyncCodesClass, REDCAP_PROJECT_KEYS
from tests.helpers.unittest_base import BaseTestCase

PROJECT_ROOT = os.path.dirname(os.path.dirname(rdr_service.__file__))


class CopeAnswerTest(BaseTestCase):
    def setUp(self):
        super().setUp()

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

    def test_code_sync(self):
        self.run_tool([
            self._get_mock_dictionary_item(
                'participant_id',
                'Participant ID',
                'text'
            ),
            self._get_mock_dictionary_item(
                'radio',
                'This is a single select question',
                'radio',
                answers='1, Choice One | 2, Choice Two | 3, Choice Three | 4, Etc.'
            )
        ])

        codes = self.session.query(Code).all()
        print(codes)
