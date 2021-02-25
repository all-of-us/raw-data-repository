from sqlalchemy import MetaData
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sphinx.pycode import ModuleAnalyzer

from rdr_service.model.base import Base
from rdr_service.services.google_sheets_client import GoogleSheetsClient
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'update-data-dictionary'
tool_desc = 'Update the RDR data-dictionary'


class UpdateDataDictionary(ToolBase):
    def __init__(self, *args, **kwargs):
        super(UpdateDataDictionary, self).__init__(*args, **kwargs)
        self.internal_table_list = [
            'alembic_version',
            'metrics_age_cache_bak',
            'metrics_enrollment_status_cache_bak',
            'metrics_gender_cache_bak',
            'metrics_lifecycle_cache_bak',
            'metrics_race_cache_bak',
            'metrics_region_cache_bak',
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

    # TODO:
    #  Create ability to clear cells in content and further rows (if the update is shorter than what was there)

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

    def _get_column_docstring_list(self, column_definition, analyzer: ModuleAnalyzer):
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

    def _get_deprecation_status_and_note(self, table_name):
        if table_name in self._deprecated_table_map:
            return True, self._deprecated_table_map[table_name]

        return False, None

    def _write_to_sheet(self, sheet: GoogleSheetsClient, tab_id, current_row, reflected_table_name, reflected_column,
                        column_description, display_unique_data, value_meaning_map, session):
        sheet.set_current_tab(tab_id)
        sheet.update_cell(current_row, 0, reflected_table_name)
        sheet.update_cell(current_row, 1, reflected_column.name)
        sheet.update_cell(current_row, 2, f'{reflected_table_name}.{reflected_column.name}')
        sheet.update_cell(current_row, 3, str(reflected_column.type))
        sheet.update_cell(current_row, 4, column_description)

        if display_unique_data:
            distinct_values = session.execute(f'select distinct {reflected_column.name} from {reflected_table_name}')
            unique_values_display_list = [str(value) if value is not None else 'NULL' for (value,) in distinct_values]

            sheet.update_cell(current_row, 5, str(len(unique_values_display_list)))
            sheet.update_cell(current_row, 6, ', '.join(
                sorted(unique_values_display_list, key=lambda val_str: val_str.lower())
            ))

        if value_meaning_map:
            sheet.update_cell(current_row, 7, value_meaning_map)

        sheet.update_cell(current_row, 8, ' ')
        sheet.update_cell(current_row, 9, 'Yes' if reflected_column.primary_key else 'No')
        sheet.update_cell(current_row, 10, 'Yes' if len(reflected_column.foreign_keys) > 0 else 'No')
        # Display the targets of foreign keys as target_table_name.target_column_name
        sheet.update_cell(current_row, 11, ', '.join([
            f'{foreign_key.column.table}.{foreign_key.column.name}' for foreign_key in reflected_column.foreign_keys
        ]))
        # Display the target column names of foreign keys
        sheet.update_cell(current_row, 12, ', '.join([
            foreign_key.column.name.ljust(20) for foreign_key in reflected_column.foreign_keys
        ]))

        is_deprecated, deprecation_note = self._get_deprecation_status_and_note(reflected_table_name)
        if is_deprecated:
            sheet.update_cell(current_row, 13, f'Deprecated: {deprecation_note}')

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

    def run(self):
        super(UpdateDataDictionary, self).run()

        dictionary_tab_id = 'RDR Data Dictionary'
        internal_tables_tab_id = 'RDR Internal Only'
        current_row_tracker = {
            dictionary_tab_id: 4,
            internal_tables_tab_id: 4
        }
        with GoogleSheetsClient(
            '1cmFnjyIqBHNbRmJ677WJjkAcGc0y2I7yfcUfpoOm1X4',
            self.gcp_env.service_key_id,
            tab_offsets={
                internal_tables_tab_id: 'B1'
            }
        ) as sheet:
            with self.get_session(alembic=True) as session:
                metadata = MetaData()
                metadata.reflect(bind=session.bind)  # , views=True)

                for table_name in sorted(metadata.tables.keys()):
                    table_data = metadata.tables[table_name]

                    print('==========================')
                    print('--------', table_name, '------------')



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
                                    column_description =\
                                        'When that record was created in the history table specifically (if main ' \
                                        'table is updated; previous version if/when a record is updated; if never ' \
                                        'changed, it appears as it was originally created)'
                            else:
                                column_description = self._get_column_description(column_definition, analyzer)

                        if self._get_is_internal_column(model, table_name, column_definition, analyzer):
                            sheet_tab_id = internal_tables_tab_id
                        else:
                            sheet_tab_id = dictionary_tab_id

                        self._write_to_sheet(sheet, sheet_tab_id, current_row_tracker[sheet_tab_id], table_name, column,
                                             column_description, show_unique_values, value_meaning_map, session)
                        current_row_tracker[sheet_tab_id] += 1


def add_additional_arguments(_):
    pass


def run():
    return cli_run(tool_cmd, tool_desc, UpdateDataDictionary, add_additional_arguments)
