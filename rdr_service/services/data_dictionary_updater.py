from datetime import datetime
from protorpc import messages
from sqlalchemy import MetaData
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sphinx.pycode import ModuleAnalyzer

from rdr_service.model.base import Base
from rdr_service.model.code import Code
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.questionnaire import QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.model.site import Site
from rdr_service.services.google_sheets_client import GoogleSheetsClient

changelog_tab_id = 'Change Log'
dictionary_tab_id = 'RDR Data Dictionary'
internal_tables_tab_id = 'RDR Internal Only'
hpo_key_tab_id = 'Key_HPO'
questionnaire_key_tab_id = 'Key_Questionnaire'
site_key_tab_id = 'Key_Site'


class DictionarySchemaField(messages.Enum):
    TABLE_NAME = 0
    COLUMN_NAME = 1
    TABLE_COLUMN_CONCATENATION = 2
    DATA_TYPE = 3
    DESCRIPTION = 4
    NUM_UNIQUE_VALUES = 5
    UNIQUE_VALUE_LIST = 6
    VALUE_MEANING_MAP = 7
    VALUES_KEY = 8
    PRIMARY_KEY_INDICATOR = 9
    FOREIGN_KEY_INDICATOR = 10
    FOREIGN_KEY_TARGET_TABLE_COLUMN_LIST = 11
    FOREIGN_KEY_TARGET_COLUMN_LIST = 12
    DEPRECATION_INDICATOR = 13
    RDR_VERSION_INTRODUCED = 14


class DictionarySchemaRowUpdateHelper:
    """Updating the rows for a column depends on shared values, this helps de-clutter while allowing for reuse"""
    def __init__(self, sheet, row, existing_row_values, changelog, changelog_key):
        self.sheet = sheet
        self.row = row
        self.existing_row_values = existing_row_values
        self.changelog = changelog
        self.changelog_key = changelog_key

    def set_value(self, value_reference, new_value):
        value_ref_index = int(value_reference)

        # Only record the update in the changelog if we're updating a row (rather than adding a brand new one)
        if self.existing_row_values:
            previous_value = self.existing_row_values[value_ref_index] \
                if len(self.existing_row_values) > value_ref_index else None
            if previous_value != new_value and (previous_value or new_value):
                if self.changelog_key not in self.changelog:
                    # Initialize this column's list of changes
                    self.changelog[self.changelog_key] = []

                self.changelog[self.changelog_key].append(f'{value_reference}: changing from: "{previous_value}" '
                                                          f'to "{new_value}"')

        self.sheet.update_cell(self.row, value_ref_index, new_value)


class KeyTabUpdateHelper:
    def __init__(self, sheet):
        self.change_detected = False
        self.sheet = sheet

    def update_with_values(self, row, values_list):
        existing_values = self.sheet.get_row_at(row)
        for column_index, new_value in enumerate(values_list):
            new_value_str = str(new_value)
            existing_value = existing_values[column_index] if len(existing_values) > column_index else None
            self.change_detected = self.change_detected or (
                existing_value != new_value_str and bool(existing_value or new_value_str)
            )
            self.sheet.update_cell(row, column_index, new_value_str)


class DataDictionaryUpdater:
    def __init__(self, gcp_service_key_id, dictionary_sheet_id, rdr_version, session=None):
        self.gcp_service_key_id = gcp_service_key_id
        self.dictionary_sheet_id = dictionary_sheet_id
        self.session = session
        self.schema_tab_row_trackers = {
            dictionary_tab_id: 4,
            internal_tables_tab_id: 4
        }
        self.rdr_version = rdr_version

        # List out tables that should be marked as internal, but aren't mapped in the ORM
        self.internal_table_list = [
            'alembic_version',
            'metrics_age_cache_bak',
            'metrics_enrollment_status_cache_bak',
            'metrics_gender_cache_bak',
            'metrics_lifecycle_cache_bak',
            'metrics_race_cache_bak',
            'metrics_region_cache_bak',
            'ptsc_cohort',
            'temp_debug_bigquery_sync',
            'tmp_participants'
        ]
        # List out deprecated tables that aren't mapped in the ORM,
        # giving notes about why it's deprecated and its replacement if there is one
        self._deprecated_table_map = {
            'biobank_dv_order': 'use biobank_mail_kit_order table instead',
            'biobank_dv_order_history': 'use biobank_mail_kit_order_history table instead'
        }
        # List of history tables that have been created using rdr_service.model.base.add_table_history_table.
        # Used to switch to non-history table for ORM descriptions since these tables aren't represented in ORM
        self._alembic_history_table_list = [
            'biobank_mail_kit_order_history',
            'genomic_set_member_history',
            'participant_gender_answers_history',
            'participant_race_answers_history',
            'patient_status_history'
        ]

        # Keep a record of the changes made to the sheet.
        # The changelog for the key tabs will just be an indicator of whether something was added or not.
        # The schema changelogs will be dictionaries. Keys will be a tuple of the table and column names,
        # and the values will be a list of the changes being made to that column
        self.changelog = {
            dictionary_tab_id: {},
            internal_tables_tab_id: {},
            hpo_key_tab_id: False,
            questionnaire_key_tab_id: False,
            site_key_tab_id: False
        }

        self._sheet = None

    def _is_alembic_generated_history_table(self, table_name):
        return table_name in self._alembic_history_table_list

    def _get_class_for_table(self, table_name):
        if self._is_alembic_generated_history_table(table_name):
            table_name = table_name[:-8]  # Trim "_history" from the table name for finding the corresponding ORM model

        for model in Base._decl_class_registry.values():
            if getattr(model, '__tablename__', '') == table_name:
                return model

    @classmethod
    def _get_column_definition_from_model(cls, model, column_name):
        for attribute in model.__dict__.values():
            if isinstance(attribute, InstrumentedAttribute) and attribute.expression.key == column_name:
                return attribute

    @classmethod
    def _trim_whitespace_lines_from_ends_of_list(cls, strings_list):
        content_lines = []
        empty_lines_in_content = []
        for line in strings_list:
            line = line.strip()

            if line:
                # Found a line with content, append it to the result
                # (preceded by any empty lines between this and previous content)
                content_lines.extend(empty_lines_in_content)
                empty_lines_in_content.clear()

                content_lines.append(line)
            else:
                # Empty line, ignore if before any content
                if content_lines:
                    empty_lines_in_content.append(line)

        return content_lines

    @classmethod
    def _get_column_docstring_list(cls, column_definition, analyzer: ModuleAnalyzer):
        if column_definition.__doc__:
            return column_definition.__doc__.split('\n')
        else:
            # Search for field definitions, starting with the class name and working up through the inheritance path
            class_names = [column_definition.class_.__name__]
            class_names.extend([base.__name__ for base in column_definition.class_.__bases__])
            for class_name in class_names:
                for (attr_doc_class_name, field_name), doc_str_list in analyzer.find_attr_docs().items():
                    if class_name == attr_doc_class_name and column_definition.key == field_name:
                        return doc_str_list

        return []

    def _get_column_description(self, column_definition, analyzer: ModuleAnalyzer):
        return '\n'.join([line for line in self._trim_whitespace_lines_from_ends_of_list(
            self._get_column_docstring_list(column_definition, analyzer)
        ) if not line.startswith('@rdr_dictionary')])

    def _get_deprecation_status_and_note(self, table_name, column_definition, analyzer: ModuleAnalyzer):
        if table_name in self._deprecated_table_map:
            return True, self._deprecated_table_map[table_name]

        if column_definition and analyzer:
            for docstring_line in self._get_column_docstring_list(column_definition, analyzer):
                if docstring_line.startswith('@deprecated'):
                    return True, docstring_line[11:].strip()

        return False, None

    def _write_to_schema_sheet(self, tab_id, reflected_table_name, reflected_column, column_description,
                               display_unique_data, value_meaning_map, is_deprecated, deprecation_note):
        self._sheet.set_current_tab(tab_id)
        current_row = self.schema_tab_row_trackers[tab_id]
        change_log_key = (reflected_table_name, reflected_column.name)
        changelog_for_tab = self.changelog[tab_id]

        # Check what's already on the sheet in the row we're currently at. If the current table and column would go
        # before what's already there, then insert a new row and fill that out. If what is there would be
        # removed (the current table or column name is different and we should have already seen what's in the sheet
        # since they're alphabetical) then remove the current row and continue checking against the next row.
        adding_new_row = False
        existing_table_name = existing_column_name = existing_row_values = None
        while not adding_new_row and not (
            # Checking if we're supposed to update the existing dictionary row
            reflected_table_name == existing_table_name and reflected_column.name == existing_column_name
        ):
            existing_row_values = self._sheet.get_row_at(current_row)
            # Check if the current row has any schema information, if not then overwrite it
            # and assume we're adding a new row
            if len(existing_row_values) < 2 or existing_row_values[:2] == ['', '']:
                adding_new_row = True
            else:
                existing_table_name, existing_column_name, *_ = existing_row_values
                if reflected_table_name < existing_table_name or (
                    reflected_table_name == existing_table_name and reflected_column.name < existing_column_name):
                    # The row being written would go before what's already there (regardless of whether what is
                    # there will continue to be there later).
                    # Insert the new row above what's already there.
                    self._sheet.insert_new_row_at(current_row)
                    adding_new_row = True
                else:
                    # At this point the table and column name we're writing either match or belong after what is
                    # currently there. If it belongs after, then we never saw what is there and we should remove the
                    # row in the sheet and continue checking the next row.
                    if reflected_table_name != existing_table_name or reflected_column.name != existing_column_name:
                        self._sheet.remove_row_at(current_row)
                        changelog_for_tab[(existing_table_name, existing_column_name)] = 'removing'

        existing_deprecation_note = None
        if adding_new_row:
            self._sheet.update_cell(current_row, DictionarySchemaField.TABLE_NAME, reflected_table_name)
            self._sheet.update_cell(current_row, DictionarySchemaField.COLUMN_NAME, reflected_column.name)
            self._sheet.update_cell(current_row, DictionarySchemaField.RDR_VERSION_INTRODUCED, self.rdr_version)
            self._sheet.update_cell(current_row, DictionarySchemaField.TABLE_COLUMN_CONCATENATION,
                                   f'{reflected_table_name}.{reflected_column.name}')
            changelog_for_tab[(reflected_table_name, reflected_column.name)] = 'adding'
        else:
            # If we're not adding a row, then we're updating one. Check to see if there's a deprecation note.
            deprecation_note_index = int(DictionarySchemaField.DEPRECATION_INDICATOR)
            if len(existing_row_values) > deprecation_note_index:
                existing_deprecation_note = existing_row_values[deprecation_note_index]

        sheet_row = DictionarySchemaRowUpdateHelper(
            self._sheet,
            current_row,
            existing_row_values if not adding_new_row else None,
            changelog_for_tab,
            change_log_key
        )

        sheet_row.set_value(DictionarySchemaField.DATA_TYPE, str(reflected_column.type))
        sheet_row.set_value(DictionarySchemaField.DESCRIPTION, column_description)

        if display_unique_data:
            distinct_values = self.session.execute(
                f'select distinct {reflected_column.name} from {reflected_table_name}'
            )
            unique_values_display_list = [str(value) if value is not None else 'NULL' for (value,) in distinct_values]

            sheet_row.set_value(DictionarySchemaField.NUM_UNIQUE_VALUES, str(len(unique_values_display_list)))
            sheet_row.set_value(DictionarySchemaField.UNIQUE_VALUE_LIST, ', '.join(
                sorted(unique_values_display_list, key=lambda val_str: val_str.lower())
            ))
        else:
            # Write empty values to the cells in case they previously had values
            sheet_row.set_value(DictionarySchemaField.NUM_UNIQUE_VALUES, '')
            sheet_row.set_value(DictionarySchemaField.UNIQUE_VALUE_LIST, '')

        sheet_row.set_value(DictionarySchemaField.VALUE_MEANING_MAP, value_meaning_map or '')
        sheet_row.set_value(DictionarySchemaField.VALUES_KEY, '')

        sheet_row.set_value(DictionarySchemaField.PRIMARY_KEY_INDICATOR,
                            'Yes' if reflected_column.primary_key else 'No')
        sheet_row.set_value(DictionarySchemaField.FOREIGN_KEY_INDICATOR,
                            'Yes' if len(reflected_column.foreign_keys) > 0 else 'No')

        # Display the targets of foreign keys as target_table_name.target_column_name
        sheet_row.set_value(DictionarySchemaField.FOREIGN_KEY_TARGET_TABLE_COLUMN_LIST, ', '.join([
            f'{foreign_key.column.table}.{foreign_key.column.name}' for foreign_key in reflected_column.foreign_keys
        ]))
        # Display the target column names of foreign keys
        sheet_row.set_value(DictionarySchemaField.FOREIGN_KEY_TARGET_COLUMN_LIST, ', '.join([
            foreign_key.column.name for foreign_key in reflected_column.foreign_keys
        ]))

        # Don't replace the existing deprecation note (and rdr version) if it's already there
        if is_deprecated and not existing_deprecation_note:
            sheet_row.set_value(DictionarySchemaField.DEPRECATION_INDICATOR,
                                f'Deprecated in {self.rdr_version}: {deprecation_note}')

        self.schema_tab_row_trackers[tab_id] += 1

    def _get_is_internal_column(self, model, table_name, column_definition, analyzer: ModuleAnalyzer):
        if model and getattr(model, '__rdr_internal_table__', False):
            return True
        elif column_definition and analyzer and any([
            docstring_line == '@rdr_dictionary_internal_column'
            for docstring_line in self._get_column_docstring_list(column_definition, analyzer)
        ]):
            return True
        else:
            return table_name in self.internal_table_list or table_name.startswith('metrics_tmp_participant')

    def _get_column_should_show_unique_values(self, column_definition, analyzer: ModuleAnalyzer):
        if column_definition and analyzer:
            for docstring_line in self._get_column_docstring_list(column_definition, analyzer):
                if docstring_line == '@rdr_dictionary_show_unique_values':
                    return True

        return False

    def _populate_questionnaire_key_tab(self):
        update_helper = KeyTabUpdateHelper(self._sheet)

        questionnaire_data_list = self.session.query(
            QuestionnaireConcept.questionnaireId,
            Code.display,
            Code.value,
            Code.shortValue,
            QuestionnaireResponse.questionnaireResponseId.isnot(None)
        ).join(
            Code,
            QuestionnaireConcept.codeId == Code.codeId
        ).join(
            QuestionnaireResponse,
            # Specifically not joining by version (and assuming all versions use the same concept) since the output
            # doesn't show version information
            QuestionnaireResponse.questionnaireId == QuestionnaireConcept.questionnaireId,
            isouter=True
        ).order_by(QuestionnaireConcept.questionnaireId).distinct().all()
        self._sheet.set_current_tab(questionnaire_key_tab_id)
        for row_number, (questionnaire_id, code_display, code_value,
                         code_short_value, has_responses) in enumerate(questionnaire_data_list):
            has_responses_yn_indicator = 'Y' if has_responses else 'N'

            is_ppi_survey = 'Scheduling' not in code_value and 'SNAP' not in code_value
            is_ppi_survey_yn_indicator = 'Y' if is_ppi_survey else 'N'

            update_helper.update_with_values(row_number, [
                questionnaire_id, code_display, code_short_value, has_responses_yn_indicator, is_ppi_survey_yn_indicator
            ])

        self._sheet.truncate_tab_at_row(len(questionnaire_data_list))
        self.changelog[questionnaire_key_tab_id] = update_helper.change_detected

    def _populate_hpo_key_tab(self):
        update_helper = KeyTabUpdateHelper(self._sheet)

        hpo_data_list = self.session.query(HPO.hpoId, HPO.name, HPO.displayName).order_by(HPO.hpoId).all()
        self._sheet.set_current_tab(hpo_key_tab_id)
        for row_number, hpo_data in enumerate(hpo_data_list):
            update_helper.update_with_values(row_number, hpo_data)

        self._sheet.truncate_tab_at_row(len(hpo_data_list))
        self.changelog[hpo_key_tab_id] = update_helper.change_detected

    def _populate_site_key_tab(self):
        update_helper = KeyTabUpdateHelper(self._sheet)

        site_data_list = self.session.query(
            Site.siteId, Site.siteName, Site.googleGroup, Organization.externalId, Organization.displayName
        ).join(Organization).order_by(Site.siteId).all()
        self._sheet.set_current_tab(site_key_tab_id)
        for row_number, site_data in enumerate(site_data_list):
            update_helper.update_with_values(row_number, site_data)

        self._sheet.truncate_tab_at_row(len(site_data_list))
        self.changelog[site_key_tab_id] = update_helper.change_detected

    def _populate_schema_tabs(self):
        metadata = MetaData()
        metadata.reflect(bind=self.session.bind, views=True)

        for table_name in sorted(metadata.tables.keys()):
            table_data = metadata.tables[table_name]

            model = self._get_class_for_table(table_name)
            analyzer = ModuleAnalyzer.for_module(model.__module__) if model else None

            for column in sorted(table_data.columns, key=lambda col: col.name):

                column_description = ''
                show_unique_values = False
                value_meaning_map = None
                column_definition = None
                if model:
                    column_definition = self._get_column_definition_from_model(model, column.name)
                    if column_definition:
                        enum_definition = getattr(column_definition.expression.type, 'enum_type', None)
                        if enum_definition:
                            value_meaning_map = ', '.join([
                                f'{str(option)} = {int(option)}' for option in enum_definition
                            ])

                        if enum_definition or \
                                self._get_column_should_show_unique_values(column_definition, analyzer):
                            show_unique_values = True

                    if self._is_alembic_generated_history_table(table_name) and column.name in [
                        'revision_action', 'revision_id', 'revision_dt'
                    ]:
                        if column.name == 'revision_action':
                            column_description = 'What operation was done for that record - INSERT or UPDATE'
                        elif column.name == 'revision_id':
                            column_description = 'Auto-incremented value used with the id column as the ' \
                                                 'primary key for the history table'
                        elif column.name == 'revision_dt':
                            column_description = \
                                'When that record was created in the history table specifically (if main ' \
                                'table is updated; previous version if/when a record is updated; if never ' \
                                'changed, it appears as it was originally created)'
                    else:
                        column_description = self._get_column_description(column_definition, analyzer)

                if self._get_is_internal_column(model, table_name, column_definition, analyzer):
                    sheet_tab_id = internal_tables_tab_id
                else:
                    sheet_tab_id = dictionary_tab_id

                is_deprecated, deprecation_note = self._get_deprecation_status_and_note(
                    table_name, column_definition, analyzer
                )

                self._write_to_schema_sheet(sheet_tab_id, table_name, column, column_description,
                                            show_unique_values, value_meaning_map, is_deprecated, deprecation_note)

        for tab_id, row in self.schema_tab_row_trackers.items():
            self._sheet.truncate_tab_at_row(row, tab_id)

        now = datetime.now()
        current_date_string = f'{now.month}/{now.day}/{now.year}'
        if self.changelog[dictionary_tab_id]:
            self._sheet.update_cell(1, 0, f'Last Updated: {current_date_string}', dictionary_tab_id)
        if self.changelog[internal_tables_tab_id]:
            self._sheet.update_cell(1, 1, f'Last Updated: {current_date_string}', internal_tables_tab_id)

    def _modify_sheet(self):
        self._populate_schema_tabs()
        self._populate_hpo_key_tab()
        self._populate_site_key_tab()
        self._populate_questionnaire_key_tab()

    def _build_sheet(self):
        return GoogleSheetsClient(
            self.dictionary_sheet_id,
            self.gcp_service_key_id,
            tab_offsets={
                changelog_tab_id: 'A2',
                dictionary_tab_id: 'B1',
                internal_tables_tab_id: 'B1',
                hpo_key_tab_id: 'A2',
                questionnaire_key_tab_id: 'A2',
                site_key_tab_id: 'A2'
            }
        )

    def run_update(self):
        with self._build_sheet() as sheet:
            self._sheet = sheet
            self._modify_sheet()

    def download_dictionary_values(self):
        self._sheet = self._build_sheet()
        self._sheet.download_values()

    def find_data_dictionary_diff(self):
        """
        This will find the updates needed for the data-dictionary and return the changes.
        Use the `upload_changes` method to write them to the data-dictionary spreadsheet.
        """
        self._modify_sheet()
        return self.changelog

    def upload_changes(self, message, author):
        if not self._sheet:
            raise Exception('Must call `find_data_dictionary_diff` first')

        self._sheet.service_key_id = self.gcp_service_key_id
        self._sheet.set_current_tab(changelog_tab_id)

        # Go through the existing change log rows until we get to a new row
        change_log_row_index = 0
        change_log_row_display = 1
        previous_change_log_row_display = self._sheet.get_row_at(change_log_row_index)[0]
        while previous_change_log_row_display != '':
            if previous_change_log_row_display.isdigit():
                change_log_row_display = int(previous_change_log_row_display) + 1
            else:
                change_log_row_display += 1

            change_log_row_index += 1
            previous_change_log_row_display = self._sheet.get_row_at(change_log_row_index)[0]

        # Write a new record to the change log tab
        self._sheet.update_cell(change_log_row_index, 0, str(change_log_row_display))
        self._sheet.update_cell(change_log_row_index, 1, message)
        today = datetime.today()
        self._sheet.update_cell(change_log_row_index, 2, f'{today.month}/{today.day}/{today.year}')
        self._sheet.update_cell(change_log_row_index, 3, self.rdr_version)
        self._sheet.update_cell(change_log_row_index, 4, author)

        # Upload all the changes
        self._sheet.upload_values()
