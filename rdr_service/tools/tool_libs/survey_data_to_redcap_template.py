import argparse
import csv

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import importlib
import logging
import sys
import json

from collections import OrderedDict

from rdr_service.code_constants import PMI_SKIP_CODE
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.model import BQ_TABLES
from rdr_service.model.survey import SurveyQuestionType

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "survey-data-to-redcap"
tool_desc = "Extracts module response data into a REDCap-conformant format for REDCap import"

class SurveyToRedCapConversion(object):


    # Static class variables / dicts that will be populated as values are discovered, so they can act as a class cache
    code_display_values = {}
    pdr_table_mod_classes = {}

    def __init__(self, args, gcp_env: GCPEnvConfigObject, module='sdoh'):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.module = module
        self.set_bq_table(module)
        self.question_code_map = OrderedDict()
        self.option_code_display_strings = dict()
        self.redcap_export_rows = list()

    def _generate_response_id_list(self, num_records=20, min_authored=None, max_authored=None):
        """
        Select a sample of questionnaire_response_id values for analysis
        """
        sql = """
               select bqs.pk_id from bigquery_sync bqs
               join questionnaire_response qr on qr.questionnaire_response_id = bqs.pk_id
               where table_id = :table and qr.classification_type = :classification
               limit :records
        """
        if min_authored or max_authored:
            pass

        with CodeDao().session() as session:
            results = session.execute(sql, {'table': self.bq_table.get_name().lower(), 'classification': 0,
                                            'records': num_records})
            return [r.pk_id for r in results]

    def get_pdr_bq_table_id(self):
        """ Return the table_id string associated with the module in the RDR bigquery_sync table generated records """
        if not self.bq_table:
            _logger.error('This object instance does not have a bq_table attribute set')
            return None
        return self.bq_table.get_name().lower()

    def set_bq_table(self, module):
        """ Set the instance bq_table variable with the appropriate _BQModuleSchema object """
        self.bq_table = None

        # Check the local class cache first for a matching module name
        if module in self.pdr_table_mod_classes.keys():
            self.bq_table = self.pdr_table_mod_classes[module]()
            return

        # If there was no match in the class cache, search the defined BQ_TABLES list for a match
        table_id = f'pdr_mod_{module.lower()}'
        for path, var_name in BQ_TABLES:
            mod = importlib.import_module(path, var_name)
            mod_class = getattr(mod, var_name)
            bq_table = mod_class()
            if bq_table.get_name().lower() == table_id:
                self.bq_table = bq_table
                # Cache the mod_class match for this module
                self.pdr_table_mod_classes[module] = mod_class
                return

        raise ValueError(f'A PDR BQ_TABLES table definition for module {module} was not found')

    def create_survey_code_maps(self, module):
        """

        """
        sql = """
            select
            sq.id question_id, sq.code_id question_code, cq.value question_code_value, cq.display question_display,
            sq.question_type, sqo.code_id option_code, co.value option_code_value,
            sqo.question_id option_question_id, co.display as option_display
            from survey s
            join survey_question sq on s.id = sq.survey_id
            join code cq on sq.code_id = cq.code_id
            left join survey_question_option sqo on sq.id = sqo.question_id
            left join code co on sqo.code_id = co.code_id
            where s.id = (
                select id from survey s
                join code cs on s.code_id = cs.code_id
                where cs.value = :module and s.replaced_time is null
                order by import_time desc
                limit 1
                )
            order by sq.id, sqo.id;
        """

        with CodeDao().session() as session:
            results = session.execute(sql, {'module': module})
            last_question_code_value = None
            for row in results:
                if row.question_code_value == last_question_code_value:
                    self.question_code_map[last_question_code_value]['option_codes'].append(row.option_code_value)
                    self.option_code_display_strings[row.option_code_value] = row.option_display
                else:
                    self.question_code_map[row.question_code_value] = {
                        'question_type': SurveyQuestionType(row.question_type),
                        'option_codes': [row.option_code_value, ] if row.option_code_value else None
                    }
                    if row.option_code_value:
                        self.option_code_display_strings[row.option_code_value] = row.option_display
                last_question_code_value = row.question_code_value

        self.redcap_export_rows.append(['record_id', ])
        for field_name, field_details in self.question_code_map.items():
            if field_details['question_type'] in (SurveyQuestionType.TEXT, SurveyQuestionType.RADIO):
                self.redcap_export_rows.append([self.get_redcap_fieldname(field_name, parent=None), ])
            elif field_details['question_type'] == SurveyQuestionType.CHECKBOX:
                for code in field_details['option_codes']:
                    self.redcap_export_rows.append([self.get_redcap_fieldname(code, parent=field_name), ])


    def get_code_display_value(self, code):
        """
        Return the display string from the code table for the given code value.  Used for exporting responses to
        radio button questions
        """
        # Check the class cache first for codes that were already discovered/stored
        if code in self.code_display_values.keys():
            return self.code_display_values[code]

        with CodeDao().session() as session:
            row = session.execute(f'select display from code where value = "{code}"').first()

        if row:
            # Cache the display value for this code, for future use
            self.code_display_values[code] = row.display
            return row.display

        _logger.warning(f'Display value requested for Unknown code {code}')
        return None

    def get_redcap_fieldname(self, code, parent=None):
        """
        Based on the code string and parent code string, generate a REDCap field name
        Ex:  code = 'TheBasics_Birthplace' where parent_code = None: returns 'thebasics_birthplace'
             code = 'WhatRaceEthnicity_AIAN', where parent_code = 'Race_WhatRaceEthnicity':
                            returns  'race_whatraceethnicity___whatraceethnicity_aian'
        :param code: Value string from the RDR code table (code.value)
        :param parent_code: A parent question code value string from the RDR code table, if the code param is an
                            option menu answer code
        :return: A string in the expected REDCap field name format
        """
        if not parent:
            return code.lower()
        else:
            return "".join([parent.lower(), f'___{code.lower()}'])

    # TODO:  Unused for SDOH but leaving in in case/until we try another module where a checkbox option list includes
    # a "prefer not to answer" option.   May still need to generate the right REDCap field name based on the option
    # menu's parent question code
    def redcap_prefer_not_to_answer(self, question_code):
        """
        The PMI_PreferNotToAnswer answer code is associated with multiple surveys and survey questions.
        When associated with a multi-select question option, the corresponding REDCap field name is based on the
        survey question code (e.g. race_whatraceethnicity___pmi_prefernottoanswer or
        gender_genderidentity___pmi_prefernottoanswer).  Those REDCap fields contain a value of 1 if
        the PMI_PreferNotToAnswer option was selected, or 0 otherwise.

        When associated with a single select/radio button question, the field name is the question code string and
        the value is the display string from the RDR code table for the PMI_PreferNotToAnswer code

        :return:  key, value where key is the REDCap field name and value is based on the question code type
        """
        pass

    def add_redcap_export_row(self, response_id, generated_redcap_dict):
        """
        Iterate over the generated REDCap field/value dict and add the data to the export.
        :param response_id: questionnaire_response_id (biquery_sync pk_id) of the survey response
        :param generated_redcap_dict: The resulting key/value pairs from the transformed PDR response data
        """

        for row in self.redcap_export_rows:
            field_name = row[0]
            if field_name == 'record_id':
                row.append(response_id)
            elif field_name in generated_redcap_dict.keys():
                row.append(generated_redcap_dict[field_name])
                # Delete recognized keys from the generated REDCap data dict after processing.  Any dict entries left
                # after finishing this for loop is a "non-conformant" field name not in the REDCap data dictionary
                del generated_redcap_dict[field_name]
            else:
                _logger.error(f'Missing field {field_name} in generated row data')
                row.append(None)
            # Capture the current row length (number of records/columns added so far)
            row_length = len(row)

        # Add a row to the export rows for any new non-conformant fields and backfill previously processed records
        for field_name in generated_redcap_dict.keys():
            _logger.error(f'Extra field name {field_name} in generated row data')
            # First column in the row is the field name, then need to backfill the already-generated records/columns
            backfill_values = [None] * (row_length - 1)
            backfill_values[-1] = generated_redcap_dict[field_name]  # Overwrite last col with this response's value
            self.redcap_export_rows.append([field_name, ] + backfill_values)

        return

    def get_module_response_dict(self, module, response_id, ro_session):
        """
        Get a PDR module data record (prepped for BigQuery/PostgreSQL) for the specified questionnaire_response_id
        TODO:  If/when the PDR generators are decoupled from RDR, then convert this to query all the
        questionnaire_response_answer data for the response_id.  May mean extending the ResponseValidator class code?
        """
        if not module or not response_id:
            print('Need module name and a questionnaire_response_id')
            return None
        elif ro_session:
            table_id = self.get_pdr_bq_table_id()
            results = ro_session.execute(('select resource from bigquery_sync '
                                          f'where table_id="{table_id}" and pk_id = {response_id}')).first()
            rsp = json.loads(results.resource) if results else None
            return rsp
        else:
            pass

        return None

    def map_response_to_redcap_dict(self, response_id, response_dict):
        """

        """
        redcap_fields = OrderedDict()
        # print(f'\n==================\n{response_id}\n==================')
        for col in self.question_code_map:
            survey_code_type = self.question_code_map[col]['question_type']
            # PDR data can have comma-separated code strings for answers to multiselect questions
            answers = list(str(response_dict[col]).split(',')) if response_dict[col] else []
            if survey_code_type == SurveyQuestionType.TEXT:
                # PDR data has already mapped null/skipped free text fields to 0 if no text was entered,
                # or 1 if text was present
                if not len(answers) or answers[0] == "0":
                    redcap_fields[self.get_redcap_fieldname(col)] = None
                elif answers[0] == "1":
                    redcap_fields[self.get_redcap_fieldname(col)] = '(redacted)'
                else:
                    # "Should never get here" but in case there was an issue with PDR data generation....
                    redcap_fields[self.get_redcap_fieldname(col)] = answers[0]
            elif survey_code_type == SurveyQuestionType.RADIO:
                if len(answers) > 1:
                    # Intentionally generate an "unsplit" string with all the answers.  This will hopefully be flagged
                    # by REDCap validation process
                    redcap_fields[col.lower()] = ','.join(answers)
                    _logger.error(f'Multiple selections found for radio button question {col} (response {response_id}')
                elif len(answers):
                    # Default to the display value associated with an option answer code
                    if answers[0] == PMI_SKIP_CODE:
                        redcap_fields[col.lower()] = None
                    else:
                        redcap_fields[col.lower()] = answers[0]
                else:
                    redcap_fields[col.lower()] = None

            elif survey_code_type == SurveyQuestionType.CHECKBOX:
                for option in self.question_code_map[col]['option_codes']:
                    field_name = "___".join([col, option.lower()])
                    redcap_fields[field_name] = int(option in answers)

        for key in redcap_fields:
            print(f'{key}:   {redcap_fields[key]}')

        # print('\n\n')
        self.add_redcap_export_row(response_id, redcap_fields)

    def export_redcap_csv(self):
        """ Write the generated REDCap export rows to a file """
        file_name = f'{self.module}_redcap_export.csv'
        print(f'Outputting results to {file_name}...')
        with open(file_name, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',')
            for row in self.redcap_export_rows:
                csv_writer.writerow(row)

    def execute(self):
        """ Run the survey-to-redcap export conversion tool """

        self.gcp_env.activate_sql_proxy(replica=True)
        dao = BigQuerySyncDao()
        self.set_bq_table(self.module)
        self.create_survey_code_maps(self.module)
        with dao.session() as session:
            # TODO:  Replace with a call to _get_response_id_list based on parameters passed
            response_list = self._generate_response_id_list()
            for rsp_id in response_list:
                rsp = self.get_module_response_dict(self.module, rsp_id, session)
                if rsp:
                    self.map_response_to_redcap_dict(rsp_id, rsp)

        self.export_redcap_csv()

        return 0

def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = SurveyToRedCapConversion(args, gcp_env, 'sdoh')
        exit_code = process.execute()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
