from rdr_service.services.data_dictionary_updater import DataDictionaryUpdater, dictionary_tab_id,\
    internal_tables_tab_id, hpo_key_tab_id, questionnaire_key_tab_id, site_key_tab_id
from tests.service_tests.test_google_sheets_client import GoogleSheetsTestBase


class DataDictionaryUpdaterTest(GoogleSheetsTestBase):
    def setUp(self, **kwargs) -> None:
        super(DataDictionaryUpdaterTest, self).setUp(**kwargs)
        self.updater = DataDictionaryUpdater('', '', self.session)

    @classmethod
    def _default_tab_names(cls):
        return [
            dictionary_tab_id, internal_tables_tab_id, hpo_key_tab_id, questionnaire_key_tab_id, site_key_tab_id
        ]

    def _get_tab_rows(self, tab_id):
        for tab_data in self._get_uploaded_sheet_data():
            if tab_data['range'][1:].startswith(tab_id):  # slicing to get rid of the quot used in the reference
                return tab_data['values']

        return None

    def assert_has_row(self, table_name, column_name, tab_values, expected_data_type=None, expected_description=None,
                       expected_unique_value_count=None, expected_unique_values_list=None,
                       expected_value_meaning_map=None, expected_is_primary_key=None,
                       expected_is_foreign_key=None, expected_foreign_key_target_table_fields=None,
                       expected_foreign_key_target_columns=None, expected_deprecated_note=None):

        # Find a row with the given table and column names
        row_found = False
        for row_values in tab_values:
            row_values_dict = {
                'row_table_name': None, 'row_column_name': None, 'table_column_concat': None, 'data_type': None,
                'description': None, 'unique_value_count': None, 'unique_value_list': None, 'value_meaning_map': None,
                'values_key': None, 'is_primary_key': None, 'is_foreign_key': None,
                'foreign_key_target_table_fields': None, 'foreign_key_target_columns': None, 'deprecated_note': None
            }
            row_values_dict.update(zip(row_values_dict, row_values))

            if row_values_dict['row_table_name'] == table_name and row_values_dict['row_column_name'] == column_name:
                row_found = True
                self.assertEqual(f'{table_name}.{column_name}', row_values_dict['table_column_concat'])

                # Compare the values displayed for the column in the data-dictionary values with the expected values
                if expected_data_type:
                    self.assertEqual(expected_data_type, row_values_dict['data_type'])
                if expected_description:
                    self.assertEqual(expected_description, row_values_dict['description'])
                if expected_unique_value_count:
                    self.assertEqual(expected_unique_value_count, row_values_dict['unique_value_count'])
                if expected_unique_values_list:
                    self.assertEqual(expected_unique_values_list, row_values_dict['unique_value_list'])
                if expected_value_meaning_map:
                    self.assertEqual(expected_value_meaning_map, row_values_dict['value_meaning_map'])
                if expected_is_primary_key is not None:
                    self.assertEqual('Yes' if expected_is_primary_key else 'No', row_values_dict['is_primary_key'])
                if expected_is_foreign_key is not None:
                    self.assertEqual('Yes' if expected_is_foreign_key else 'No', row_values_dict['is_foreign_key'])
                if expected_foreign_key_target_table_fields:
                    self.assertEqual(
                        expected_foreign_key_target_table_fields,
                        row_values_dict['foreign_key_target_table_fields']
                    )
                if expected_foreign_key_target_columns:
                    self.assertEqual(expected_foreign_key_target_columns, row_values_dict['foreign_key_target_columns'])
                if expected_deprecated_note:
                    self.assertEqual(expected_deprecated_note, row_values_dict['deprecated_note'])

        if not row_found:
            self.fail(f'{table_name}.{column_name} not found in results')

    def test_updating_data_dictionary_tab(self):
        self.updater.run_update()
        dictionary_tab_rows = self._get_tab_rows(dictionary_tab_id)

        # Check for some generic columns and definitions
        self.assert_has_row(
            'participant_summary', 'deceased_status', dictionary_tab_rows,
            expected_data_type='SMALLINT(6)',
            expected_description="Indicates whether the participant has a PENDING or APPROVED deceased reports.\n\n"
                                 "Will be UNSET for participants that have no deceased reports or only DENIED reports."
        )
        self.assert_has_row(
            'biobank_stored_sample', 'disposed', dictionary_tab_rows,
            expected_data_type='DATETIME',
            expected_description="The datetime at which the sample was disposed of"
        )

    def test_show_unique_values(self):
        # Create some data for checking the dictionary values list
        self.data_generator.create_database_participant(participantOrigin='test')

        self.updater.run_update()
        dictionary_tab_rows = self._get_tab_rows(dictionary_tab_id)

        # Check that enumerations show the value meanings and unique value count
        # This check assumes that Organization's isObsolete property is based on an Enum,
        # and that the test data only has Organizations that don't have isObsolete set
        self.assert_has_row(
            'organization', 'is_obsolete', dictionary_tab_rows,
            expected_unique_values_list='NULL',
            expected_unique_value_count='1',
            expected_value_meaning_map="ACTIVE = 0, OBSOLETE = 1"
        )

        # Check that a column will show unique values when it is explicitly set to
        # This check assumes that Participant's participantOrigin is set to show unique values
        self.assert_has_row(
            'participant', 'participant_origin', dictionary_tab_rows,
            expected_unique_values_list='test',
            expected_unique_value_count='1',
            expected_value_meaning_map=''
        )

    def test_primary_and_foreign_key_columns(self):
        self.updater.run_update()
        dictionary_tab_rows = self._get_tab_rows(dictionary_tab_id)

        # Check the primary key column indicator
        self.assert_has_row('participant', 'participant_id', dictionary_tab_rows, expected_is_primary_key=True)

        # Check the foreign key column indicator
        self.assert_has_row('participant', 'site_id', dictionary_tab_rows, expected_is_foreign_key=True)

        # Check the foreign key column target fields
        self.assert_has_row(
            'participant', 'site_id', dictionary_tab_rows,
            expected_foreign_key_target_table_fields='site.site_id',
            expected_foreign_key_target_columns='site_id'
        )

    def test_internal_tab_values(self):
        self.updater.run_update()
        internal_tab_rows = self._get_tab_rows(internal_tables_tab_id)

        # Check that ORM mapped tables can appear in the internal tab when marked as internal
        self.assert_has_row('bigquery_sync', 'id', internal_tab_rows)

    def test_hpo_and_site_key_tabs(self):
        # Create hpo and site for test
        self.data_generator.create_database_hpo(hpoId=1000, name='DictionaryTest', displayName='Dictionary Test')
        self.data_generator.create_database_site(siteId=4000, siteName='Test', googleGroup='test_site_group')

        self.updater.run_update()

        # Check that the expected hpo row gets into the spreadsheet
        hpo_rows = self._get_tab_rows(hpo_key_tab_id)
        self.assertTrue([1000, 'DictionaryTest', 'Dictionary Test'] in hpo_rows)

        # Check that the expected site row gets into the spreadsheet
        site_rows = self._get_tab_rows(site_key_tab_id)
        self.assertIn([4000, 'Test', 'test_site_group'], site_rows)

    def test_questionnaire_key_tab(self):
        # Create two questionnaires for the test, one without any responses and another that has one
        code = self.data_generator.create_database_code(display='Test Questionnaire', shortValue='test_questionnaire')

        no_response_questionnaire = self.data_generator.create_database_questionnaire_history()
        response_questionnaire = self.data_generator.create_database_questionnaire_history()
        for questionnaire in [no_response_questionnaire, response_questionnaire]:
            self.data_generator.create_database_questionnaire_concept(
                questionnaireId=questionnaire.questionnaireId,
                questionnaireVersion=questionnaire.version,
                codeId=code.codeId
            )

        participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_questionnaire_response(
            questionnaireId=response_questionnaire.questionnaireId,
            questionnaireVersion=response_questionnaire.version,
            participantId=participant.participantId
        )

        # Check that the questionnaire values output as expected
        self.updater.run_update()
        questionnaire_values = self._get_tab_rows(questionnaire_key_tab_id)
        self.assertIn(
            [no_response_questionnaire.questionnaireId, code.display, code.shortValue, 'N'],
            questionnaire_values
        )
        self.assertIn(
            [response_questionnaire.questionnaireId, code.display, code.shortValue, 'Y'],
            questionnaire_values
        )
