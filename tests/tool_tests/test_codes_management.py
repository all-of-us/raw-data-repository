import json
import mock
import os

import rdr_service
from rdr_service.model.code import Code, CodeType
from rdr_service.model.survey import Survey, SurveyQuestion, SurveyQuestionType, SurveyQuestionOption
from rdr_service.tools.tool_libs._tool_base import ToolBase
from rdr_service.tools.tool_libs.codes_management import CodesSyncClass, DRIVE_EXPORT_FOLDER_ID,\
    EXPORT_SERVICE_ACCOUNT_NAME, REDCAP_PROJECT_KEYS
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
    def run_tool(redcap_data_dictionary, project_info=None, reuse_codes=[], dry_run=False, export_only=False):
        def get_server_config(*_):
            config = {
                REDCAP_PROJECT_KEYS: {
                    'project_one': '1234ABC'
                },
                DRIVE_EXPORT_FOLDER_ID: '1a789',
                EXPORT_SERVICE_ACCOUNT_NAME: 'exporter@example.com'
            }
            return json.dumps(config), 'test-file-name'

        gcp_env = mock.MagicMock()
        gcp_env.project = 'localhost'
        gcp_env.git_project = PROJECT_ROOT
        gcp_env.get_latest_config_from_bucket = get_server_config

        args = mock.MagicMock()
        args.redcap_project = 'project_one'
        args.dry_run = dry_run
        args.reuse_codes = ','.join(reuse_codes)
        args.export_only = export_only

        if project_info is None:
            project_info = {
                'project_id': 1,
                'project_title': 'Test'
            }

        with mock.patch('rdr_service.tools.tool_libs.codes_management.RedcapClient') as mock_redcap_class,\
                mock.patch('rdr_service.tools.tool_libs.codes_management.csv') as mock_csv,\
                mock.patch('rdr_service.tools.tool_libs.codes_management.open'),\
                mock.patch.object(ToolBase, 'initialize_process_context') as mock_init_env:
            mock_init_env.return_value.__enter__.return_value = gcp_env

            mock_redcap_instance = mock_redcap_class.return_value
            mock_redcap_instance.get_data_dictionary.return_value = redcap_data_dictionary
            mock_redcap_instance.get_project_info.return_value = project_info

            mock_csv_writerow = mock_csv.writer.return_value.writerow

            sync_codes_tool = CodesSyncClass(args, gcp_env)
            return sync_codes_tool.run_process(), mock_redcap_instance, mock_csv_writerow

    def assertCodeHasExpectedData(self, code: Code, expected_data):
        self.assertEqual(expected_data['type'], code.codeType)
        self.assertEqual(expected_data['value'], code.value)

    def assertCodeExists(self, value, code_type: CodeType):
        code = self.session.query(Code).filter(Code.value == value).one()
        self.assertEqual(code_type, code.codeType)

    def test_question_and_answer_codes(self):
        test_survey_project_id = 123
        test_survey_project_title = 'Survey Structure Test'
        self.run_tool([
            self._get_mock_dictionary_item('module_code', 'Test Questionnaire Module', 'descriptive'),
            self._get_mock_dictionary_item(
                'record_id',
                'Redcap inserts a record_id code into everything, it should be ignored',
                'text'
            ),
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
        ], project_info={
            'project_id': test_survey_project_id,
            'project_title': test_survey_project_title
        })
        self.assertEqual(7, self.session.query(Code).count(), '7 codes should have been created')

        survey: Survey = self.session.query(Survey).filter(Survey.redcapProjectId == test_survey_project_id).one()
        self.assertEqual(test_survey_project_title, survey.redcapProjectTitle)
        self.assertCodeHasExpectedData(survey.code, {
            'value': 'module_code',
            'type': CodeType.MODULE
        })

        self.assertEqual(2, len(survey.questions))
        for question_index, survey_question in enumerate(survey.questions):
            if question_index == 0:
                self.assertEqual('Participant ID', survey_question.display)
                self.assertEqual(SurveyQuestionType.TEXT, survey_question.questionType)
                self.assertCodeHasExpectedData(survey_question.code, {
                    'value': 'participant_id',
                    'type': CodeType.QUESTION
                })
            elif question_index == 1:
                self.assertEqual('This is a single-select, multiple choice question', survey_question.display)
                self.assertEqual(SurveyQuestionType.RADIO, survey_question.questionType)
                self.assertCodeHasExpectedData(survey_question.code, {
                    'value': 'radio',
                    'type': CodeType.QUESTION
                })

                expected_option_data_list = [
                    {'value': 'A1', 'display': 'Choice One'},
                    {'value': 'A2', 'display': 'Choice Two'},
                    {'value': 'A3', 'display': 'Choice Three'},
                    {'value': 'A4', 'display': 'Etc.'}
                ]
                self.assertEqual(len(expected_option_data_list), len(survey_question.options))
                for option_index, survey_question_option in enumerate(survey_question.options):
                    expected_option_data = expected_option_data_list[option_index]
                    self.assertEqual(expected_option_data['display'], survey_question_option.display)
                    self.assertCodeHasExpectedData(survey_question_option.code, {
                        'value': expected_option_data['value'],
                        'type': CodeType.ANSWER
                    })

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
        self.assertEqual(
            1,
            self.session.query(Code).filter(Code.codeType == CodeType.MODULE).count(),
            'Only 1 module code should have been created'
        )
        self.assertCodeExists('TestQuestionnaire', CodeType.MODULE)

    @mock.patch('rdr_service.tools.tool_libs.codes_management.logger')
    def test_failure_on_question_code_reuse(self, mock_logger):
        self.data_generator.create_database_code(value='old_code')

        return_val, _, _ = self.run_tool([
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
        self.assertEqual(0, self.session.query(Survey).count(), 'No survey objects should be created')
        self.assertEqual(0, self.session.query(SurveyQuestion).count(), 'No survey objects should be created')

        mock_logger.error.assert_any_call('Code "old_code" is already in use')
        self.assertEqual(1, return_val, 'Script should exit with an error code')

    def test_auto_ignore_answer_code_reuse(self):
        self.data_generator.create_database_code(value='A1')

        return_val, _, _ = self.run_tool([
            self._get_mock_dictionary_item('module_code', 'Test Questionnaire Module', 'descriptive'),
            self._get_mock_dictionary_item(
                'radio',
                'This is a single-select, multiple choice question',
                'radio',
                answers='A1, Choice One | A2, Choice Two | A3, Choice Three | A4, Etc.'
            )
        ])
        self.assertEqual(6, self.session.query(Code).count(), 'Should be 6 codes after test')
        self.assertEqual(0, return_val, 'Script should successfully exit, ignoring that the answer code was reused')

    def test_allowing_for_explicit_question_code_reuse(self):
        self.data_generator.create_database_code(value='TestQuestionnaire')
        self.data_generator.create_database_code(value='old_code')

        return_val, _, _ = self.run_tool([
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

    @mock.patch('rdr_service.tools.tool_libs.codes_management.logger')
    def test_dry_run(self, mock_logger):
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
                'radio',
                'This is a single-select, multiple choice question',
                'radio',
                answers='A1, Choice One | A2, Choice Two | A3, Choice Three | A4, Etc.'
            )
        ], dry_run=True)
        self.assertEqual(0, self.session.query(Code).count(), 'No codes should be created during a dry run')
        self.assertEqual(0, self.session.query(Survey).count(), 'No survey objects should be created during a dry run')
        self.assertEqual(0, self.session.query(SurveyQuestion).count(),
                         'No survey objects should be created during a dry run')
        self.assertEqual(0, self.session.query(SurveyQuestionOption).count(),
                         'No survey objects should be created during a dry run')

        mock_logger.info.assert_any_call('Found new "MODULE" type code, value: TestQuestionnaire')
        mock_logger.info.assert_any_call('Found new "QUESTION" type code, value: participant_id')
        mock_logger.info.assert_any_call('Found new "QUESTION" type code, value: radio')
        mock_logger.info.assert_any_call('Found new "ANSWER" type code, value: A1')
        mock_logger.info.assert_any_call('Found new "ANSWER" type code, value: A2')
        mock_logger.info.assert_any_call('Found new "ANSWER" type code, value: A3')
        mock_logger.info.assert_any_call('Found new "ANSWER" type code, value: A4')

    @mock.patch('rdr_service.tools.tool_libs.codes_management.logger')
    def test_dry_run_with_reuse_and_errors(self, mock_logger):
        self.data_generator.create_database_code(value='old_code')
        self.data_generator.create_database_code(value='accidental_reuse')
        self.data_generator.create_database_code(value='A2')  # Reuse should go through, but no logs should print

        _, _, mock_csv_writerow = self.run_tool([
            self._get_mock_dictionary_item(
                'TestQuestionnaire',
                'Test Questionnaire Module',
                'descriptive'
            ),
            self._get_mock_dictionary_item(
                'old_code',
                'Question reused',
                'text'
            ),
            self._get_mock_dictionary_item(
                'accidental_reuse',
                'Question reused',
                'text'
            ),
            self._get_mock_dictionary_item(
                'radio',
                'This is a single-select, multiple choice question',
                'radio',
                answers='A1, Choice One | A2, Choice Two | A3, Choice Three | A4, Etc.'
            )
        ], dry_run=True, reuse_codes=['old_code'])
        self.assertEqual(3, self.session.query(Code).count(), 'No codes should be created during a dry run')

        mock_logger.info.assert_any_call('Found new "MODULE" type code, value: TestQuestionnaire')
        mock_logger.info.assert_any_call('Found new "QUESTION" type code, value: radio')
        mock_logger.info.assert_any_call('Found new "ANSWER" type code, value: A1')
        mock_logger.info.assert_any_call('Found new "ANSWER" type code, value: A3')
        mock_logger.info.assert_any_call('Found new "ANSWER" type code, value: A4')
        self.assertEqual(5, mock_logger.info.call_count, 'Logs were made for codes that would not have been created')

        mock_logger.error.assert_any_call('Code "accidental_reuse" is already in use')

        # Make sure the export code doesn't run
        mock_csv_writerow.assert_not_called()

    def test_no_import_on_export_only(self):
        _, mock_redcap_instance, _ = self.run_tool([
            self._get_mock_dictionary_item('participant_id', 'Participant ID', 'text')
        ], export_only=True)

        # Redcap should not be called when only exporting codes
        mock_redcap_instance.get_data_dictionary.assert_not_called()
        self.assertEqual(0, self.session.query(Code).count(), 'No codes should be created when only exporting')

    def test_export_file_creation(self):
        self.data_generator.create_database_code(value='old_code', display='Code we already had')
        self.data_generator.create_database_code(value='another', display='Code we already had')
        _, _, mock_csv_writerow = self.run_tool([
            self._get_mock_dictionary_item('TestQuestionnaire', 'Test Questionnaire Module', 'descriptive'),
            self._get_mock_dictionary_item('participant_id', 'Participant ID', 'text'),
            self._get_mock_dictionary_item('radio', 'multi-select', 'radio',
                                           answers='A1, One | A2, Two | A3, Three | A4, Etc.')
        ])

        mock_csv_writerow.assert_has_calls([
            mock.call(['Code Value', 'Display', 'Parent Values', 'Module Values']),
            mock.call(['A1', 'One', 'radio', 'TestQuestionnaire']),
            mock.call(['A2', 'Two', 'radio', 'TestQuestionnaire']),
            mock.call(['A3', 'Three', 'radio', 'TestQuestionnaire']),
            mock.call(['A4', 'Etc.', 'radio', 'TestQuestionnaire']),
            mock.call(['another', 'Code we already had']),
            mock.call(['old_code', 'Code we already had']),
            mock.call(['participant_id', 'Participant ID', 'TestQuestionnaire', 'TestQuestionnaire']),
            mock.call(['radio', 'multi-select', 'TestQuestionnaire', 'TestQuestionnaire']),
            mock.call(['TestQuestionnaire', 'Test Questionnaire Module']),
        ])

    @mock.patch('rdr_service.tools.tool_libs.codes_management.logger')
    def test_module_code_is_required(self, mock_logger):
        return_val, *_ = self.run_tool([
            self._get_mock_dictionary_item('participant_id', 'Participant ID', 'text'
            )
        ])

        self.assertEqual(0, self.session.query(Code).count(), 'No codes should have been created')
        mock_logger.error.assert_any_call('No module code found, canceling import')
        self.assertEqual(1, return_val, 'Script should exit with an error code')

    @mock.patch('rdr_service.tools.tool_libs.codes_management.logger')
    def test_invalid_codes_are_rejected(self, mock_logger):
        """Code values should only have alphanumeric or underscore characters"""
        return_val, *_ = self.run_tool([
            self._get_mock_dictionary_item('module_code', 'Test Questionnaire Module', 'descriptive'),
            self._get_mock_dictionary_item('valid_code', 'Participant ID', 'text'),
            self._get_mock_dictionary_item("invalid'code", 'quote character not allowed', 'text'),
            self._get_mock_dictionary_item('another bad code', 'spaces not allowed', 'text')
        ])

        self.assertEqual(0, self.session.query(Code).count(), 'No codes should have been created')
        mock_logger.error.assert_any_call('''Invalid code values found: "invalid\'code", "another bad code"''')
        self.assertEqual(1, return_val, 'Script should exit with an error code')

    @mock.patch('rdr_service.tools.tool_libs.codes_management.logger')
    def test_missing_options_fail_import(self, mock_logger):
        """Questions that are single or multi select should have options associated with them"""
        return_val, *_ = self.run_tool([
            self._get_mock_dictionary_item('module_code', 'Test Questionnaire Module', 'descriptive'),
            self._get_mock_dictionary_item('radio_code', 'single-select', 'radio'),
            self._get_mock_dictionary_item('dropdown_code', 'dropdown', 'dropdown'),
            self._get_mock_dictionary_item('checkbox_code', 'multi-select', 'checkbox')
        ])

        self.assertEqual(0, self.session.query(Code).count(), 'No codes should have been created')
        mock_logger.error.assert_any_call('The following question codes are missing answer options: '
                                          '"radio_code", "dropdown_code", "checkbox_code"')
        self.assertEqual(1, return_val, 'Script should exit with an error code')

    def test_reimporting_project_sets_previous_record_as_obsolete(self):
        """Older Survey objects that have """
