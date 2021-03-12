from datetime import datetime

from rdr_service.services.data_dictionary_updater import DataDictionaryUpdater, changelog_tab_id, dictionary_tab_id,\
    internal_tables_tab_id, hpo_key_tab_id, questionnaire_key_tab_id, site_key_tab_id
from tests.service_tests.test_google_sheets_client import GoogleSheetsTestBase


class DataDictionaryUpdaterTest(GoogleSheetsTestBase):
    def setUp(self, **kwargs) -> None:
        super(DataDictionaryUpdaterTest, self).setUp(**kwargs)

        self.mock_rdr_version = '1.97.1'
        self.updater = DataDictionaryUpdater('', '', self.mock_rdr_version, self.session)

    @classmethod
    def _default_tab_names(cls):
        return [
            changelog_tab_id, dictionary_tab_id, internal_tables_tab_id,
            hpo_key_tab_id, questionnaire_key_tab_id, site_key_tab_id
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
                       expected_foreign_key_target_columns=None, expected_deprecated_note=None,
                       expected_rdr_version=None):

        # Find a row with the given table and column names
        row_found = False
        for row_values in tab_values:
            row_values_dict = {
                'row_table_name': None, 'row_column_name': None, 'table_column_concat': None, 'data_type': None,
                'description': None, 'unique_value_count': None, 'unique_value_list': None, 'value_meaning_map': None,
                'values_key': None, 'is_primary_key': None, 'is_foreign_key': None,
                'foreign_key_target_table_fields': None, 'foreign_key_target_columns': None, 'deprecated_note': None,
                'rdr_version': None
            }
            row_values_dict.update(zip(row_values_dict, row_values))

            if row_values_dict['row_table_name'] == table_name and row_values_dict['row_column_name'] == column_name:
                row_found = True

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
                if expected_rdr_version:
                    self.assertEqual(expected_rdr_version, row_values_dict['rdr_version'])

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

        # Check that the update date gets written
        timestamp_cell_value = dictionary_tab_rows[1][0]
        today = datetime.today()
        self.assertEqual(f'Last Updated: {today.month}/{today.day}/{today.year}', timestamp_cell_value)

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

        # Check that the update date gets written
        timestamp_cell_value = internal_tab_rows[1][1]
        today = datetime.today()
        self.assertEqual(f'Last Updated: {today.month}/{today.day}/{today.year}', timestamp_cell_value)

    def test_hpo_and_site_key_tabs(self):
        # Create hpo and site for test
        self.data_generator.create_database_hpo(hpoId=1000, name='DictionaryTest', displayName='Dictionary Test')
        test_org = self.data_generator.create_database_organization(displayName='Test Org', externalId='test_org')
        self.data_generator.create_database_site(
            siteId=4000, siteName='Test', googleGroup='test_site_group', organizationId=test_org.organizationId
        )

        self.updater.run_update()

        # Check that the expected hpo row gets into the spreadsheet
        hpo_rows = self._get_tab_rows(hpo_key_tab_id)
        self.assertIn(['1000', 'DictionaryTest', 'Dictionary Test'], hpo_rows)

        # Check that the expected site row gets into the spreadsheet
        site_rows = self._get_tab_rows(site_key_tab_id)
        self.assertIn(['4000', 'Test', 'test_site_group', test_org.externalId, test_org.displayName], site_rows)

    def test_questionnaire_key_tab(self):
        # Create two questionnaires for the test, one without any responses and another that has one
        # Also make one a scheduling survey to check the PPI survey indicator
        code = self.data_generator.create_database_code(display='Test Questionnaire', shortValue='test_questionnaire')
        scheduling_code = self.data_generator.create_database_code(
            display='Scheduling Survey',
            value='Scheduling',
            shortValue='Scheduling'
        )
        no_response_questionnaire = self.data_generator.create_database_questionnaire_history()
        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=no_response_questionnaire.questionnaireId,
            questionnaireVersion=no_response_questionnaire.version,
            codeId=code.codeId
        )
        response_questionnaire = self.data_generator.create_database_questionnaire_history()
        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=response_questionnaire.questionnaireId,
            questionnaireVersion=response_questionnaire.version,
            codeId=scheduling_code.codeId
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
            [str(no_response_questionnaire.questionnaireId), code.display, code.shortValue, 'N', 'Y'],
            questionnaire_values
        )
        self.assertIn(
            [str(response_questionnaire.questionnaireId),
             scheduling_code.display, scheduling_code.shortValue, 'Y', 'N'],
            questionnaire_values
        )

    def _mock_tab_data(self, tab_id, *rows):
        default_tab_values = {tab_id: [self._empty_cell] for tab_id in self.default_tab_names}
        default_tab_values[tab_id] = [
            *rows
        ]
        self.mock_spreadsheets_return.get.return_value.execute.return_value = {
            'sheets': [{
                'properties': {'title': tab_name},
                'data': [{'rowData': [{'values': row_values} for row_values in tab_rows]}]
            } for tab_name, tab_rows in default_tab_values.items()]
        }

    def _mock_data_dictionary_rows(self, *rows):
        self._mock_tab_data(
            dictionary_tab_id,
            [self._empty_cell],
            [self._empty_cell],
            [self._empty_cell],
            [self._empty_cell],
            *rows
        )

    def test_version_added_display(self):
        """Verify that rows for the data-dictionary show what RDR version they were added in"""

        # Set the spreadsheet up to have a previously existing record that shouldn't have the version number changed
        self._mock_data_dictionary_rows(
            # Add a row for participant id that gives RDR version 1.1 (using expansion to fill in the middle cells)
            [self._mock_cell('participant'), self._mock_cell('participant_id'),
             *([self._empty_cell] * 12), self._mock_cell('1.2.1')]
        )

        self.updater.run_update()
        dictionary_tab_rows = self._get_tab_rows(dictionary_tab_id)

        # Check that a column that wasn't already on the spreadsheet shows the new version number
        self.assert_has_row('participant', 'biobank_id', dictionary_tab_rows,
                            expected_rdr_version=self.mock_rdr_version)

        # Check that previous RDR version values are maintained
        self.assert_has_row('participant', 'participant_id', dictionary_tab_rows, expected_rdr_version='1.2.1')

    def test_version_in_deprecation_note(self):
        """Verify that rows for the data-dictionary show what RDR version they were deprecated in"""

        # Set a previously existing record that doesn't have a deprecation note, but will get one in an update
        # This test assumes there is something in a current model that is marked as deprecated.
        self._mock_data_dictionary_rows(
            # Add a row for participant_summary's ehr_status column (using expansion to fill in the middle cells)
            [self._mock_cell('participant_summary'), self._mock_cell('ehr_status')]
        )

        self.updater.run_update()
        dictionary_tab_rows = self._get_tab_rows(dictionary_tab_id)

        # Check that previous RDR version values are maintained
        self.assert_has_row('participant_summary', 'ehr_status', dictionary_tab_rows,
                            expected_deprecated_note=f'Deprecated in {self.mock_rdr_version}: '
                                                     'Use wasEhrDataAvailable (was_ehr_data_available) instead')

    def test_existing_deprecation_note_left_alone(self):
        """
        Verify that rows that already have deprecation notes don't update again (so that the version stays the same)
        """

        # Set a previously existing record that doesn't have a deprecation note, but will get one in an update
        # This test assumes there is something in a current model that is marked as deprecated.
        deprecation_note_with_version = 'Deprecated in 1.1.1: use something else'
        self._mock_data_dictionary_rows(
            # Add note for participant_summary's ehr_status column (using expansion to fill in the middle cells)
            [self._mock_cell('participant_summary'), self._mock_cell('ehr_status'),
             *([self._empty_cell] * 11), self._mock_cell(deprecation_note_with_version)]
        )

        self.updater.run_update()
        dictionary_tab_rows = self._get_tab_rows(dictionary_tab_id)

        # Check that previous RDR version values are maintained
        self.assert_has_row('participant_summary', 'ehr_status', dictionary_tab_rows,
                            expected_deprecated_note=deprecation_note_with_version)

    def test_changelog_adding_and_removing_rows(self):
        """Check that adding and removing columns from the data-dictionary gets recorded in the changelog"""

        # Create something in the data-dictionary that will be removed because it isn't in the current schema
        self._mock_data_dictionary_rows(
            [self._mock_cell('table_that_never_existed'), self._mock_cell('id')]
        )
        self.updater.run_update()

        # Check that the changelog shows that we're adding something that is in our current schema, and
        # removing the dictionary record that isn't
        data_dictionary_change_log = self.updater.changelog[dictionary_tab_id]
        self.assertEqual('adding', data_dictionary_change_log.get(('participant', 'participant_id')))
        self.assertEqual('removing', data_dictionary_change_log.get(('table_that_never_existed', 'id')))

    def test_change_log_when_updating_schema_row(self):
        """Show that the changelog displays what changed when updating a row in the data-dictionary"""

        # Mock that participant table's participant_id is a VARCHAR described as a "Random string"
        # that is not the primary key but is indicated as a foreign key to another table
        self._mock_data_dictionary_rows(
            [self._mock_cell('participant'), self._mock_cell('participant_id'), self._empty_cell,
             self._mock_cell('VARCHAR'), self._mock_cell('Random string'), *([self._empty_cell] * 5),
             self._mock_cell('Yes')]
        )
        self.updater.run_update()

        # Check the change log and verify the changes shown for the participant_id column
        data_dictionary_change_log = self.updater.changelog[dictionary_tab_id]
        list_of_participant_id_changes = data_dictionary_change_log.get(('participant', 'participant_id'))
        self.assertIn('DATA_TYPE: changing from: "VARCHAR" to "INTEGER(11)"', list_of_participant_id_changes)
        self.assertIn('DESCRIPTION: changing from: "Random string" to '
                      '"PMI-specific ID generated by the RDR and used for tracking/linking participant data.\n'
                      '10-character string beginning with P."', list_of_participant_id_changes)
        self.assertIn('PRIMARY_KEY_INDICATOR: changing from: "" to "Yes"', list_of_participant_id_changes)
        self.assertIn('FOREIGN_KEY_INDICATOR: changing from: "Yes" to "No"', list_of_participant_id_changes)

    def test_key_tab_change_indicator(self):
        self.updater.run_update()

        # For now the changelog just says whether something was changed on the key tabs.
        # Check to make sure it's set appropriately.
        # This test assumes there are no Questionnaires in the database (that way the questionnair key tab stays
        # unchanged/empty and there wouldn't be any changes for it.
        self.assertTrue(self.updater.changelog[hpo_key_tab_id])
        self.assertFalse(self.updater.changelog[questionnaire_key_tab_id])

    def test_change_log_message(self):
        # Mock some messages on the change log tab
        self._mock_tab_data(
            changelog_tab_id,
            [self._empty_cell],
            [self._mock_cell('4'), self._mock_cell('adding all fields'), self._mock_cell('1/3/20'),
             self._mock_cell('1.1.3'), self._mock_cell('test@one.com')],
            [self._mock_cell('6'), self._mock_cell('removing all fields'), self._mock_cell('10/31/20'),
             self._mock_cell('1.70.1'), self._mock_cell('test@two.com')]
        )
        self.updater.find_data_dictionary_diff()
        self.updater.upload_changes('adding them back again', 'test@three.com')

        # Check the change log values and make sure the new message was uploaded correctly
        change_log_data_uploaded = self._get_tab_rows(changelog_tab_id)
        today = datetime.today()
        self.assertEqual([
            ['4', 'adding all fields', '1/3/20', '1.1.3', 'test@one.com'],
            ['6', 'removing all fields', '10/31/20', '1.70.1', 'test@two.com'],
            ['7', 'adding them back again', f'{today.month}/{today.day}/{today.year}',
             self.mock_rdr_version, 'test@three.com']
        ], change_log_data_uploaded)
