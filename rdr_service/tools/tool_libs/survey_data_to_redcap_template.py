import argparse
import csv

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import importlib
import logging
import sys
import os

from collections import OrderedDict
from datetime import datetime

from rdr_service.code_constants import PMI_SKIP_CODE
from rdr_service.services.system_utils import setup_logging, setup_i18n, print_progress_bar, list_chunks
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.bq_questionnaire_dao import BQPDRQuestionnaireResponseGenerator
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

    # Meta data fields about the RDR/PDR questionnaire_response entry excluded from REDCap data export
    pdr_meta_data_keys = [
        'authored', 'created', 'external_id', 'language', 'participant_id', 'questionnaire_id', 'status', 'status_id',
        'questionnaire_response_id'
    ]

    def __init__(self, args, gcp_env: GCPEnvConfigObject, pid_list=None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.pid_list = pid_list
        if pid_list:
            self.num_pids = len(pid_list)
        else:
            self.num_pids = args.number or 25
        self.module = args.module
        self.survey_import_time = None
        self.bq_table = self.set_bq_table(self.module)
        self.question_code_map = OrderedDict()
        self.redcap_header_row = list()
        self.redcap_export_rows = list()

    def _generate_pid_list(self, min_authored=None, max_authored=None):
        """
        Select a sample of participants with responses for the specified module and save the participant_ids to
        self.pid_list class instance variable
        """
        min_authored = min_authored or self.survey_import_time
        max_authored = max_authored or datetime.utcnow()
        sql = """
               select qr.participant_id from questionnaire_response qr
               join questionnaire_concept qc on qc.questionnaire_id = qr.questionnaire_id
               join code c on qc.code_id = c.code_id
               join participant p on qr.participant_id = p.participant_id
               where c.value = :module and qr.classification_type = 0 and p.is_test_participant = 0
                     and qr.authored BETWEEN :min_authored and :max_authored
               limit :count
        """

        with CodeDao().session() as session:
            results = session.execute(sql, {'module': self.module, 'count': self.num_pids,
                                            'min_authored': min_authored, 'max_authored': max_authored})
            self.pid_list = [r.participant_id for r in results]

    def get_pdr_bq_table_id(self):
        """ Return the table_id string associated with the module in the RDR bigquery_sync table generated records """
        if not self.bq_table:
            _logger.error('This object instance does not have a bq_table attribute set')
            return None
        return self.bq_table.get_name().lower()

    def set_survey_import_time(self, import_time):
        """ Set the instance survey import time.  Used to filter selection of records for export """
        self.survey_import_time = import_time

    def set_bq_table(self, module):
        """ Set the instance bq_table variable with the appropriate _BQModuleSchema object """
        bq_table = None

        # Check the local class cache first for a matching module name
        if module in self.pdr_table_mod_classes.keys():
            bq_table = self.pdr_table_mod_classes[module]()
            self.bq_table = bq_table
            return bq_table

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
                return bq_table

        raise ValueError(f'A PDR BQ_TABLES table definition for module {module} was not found')

    def create_survey_code_map(self, module):
        """
        Walk the RDR survey tables to build a list of question codes and any answer option codes associated
        with those questions.  These determine the field names used for the REDCap data export
        """
        sql = """
            select
            s.import_time,
            sq.id question_id, sq.code_id question_code, cq.value question_code_value,
            sq.question_type, sqo.code_id option_code, co.value option_code_value,
            sqo.question_id option_question_id
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
            import_time = None
            for row in results:
                import_time = import_time or row.import_time
                # Question codes with radio / checkbox answer options will have multiple rows in the results
                # (one for each answer option).
                if row.question_code_value == last_question_code_value:
                    # Keep adding the answer option codes associated with the same "parent" question code
                    self.question_code_map[last_question_code_value]['option_codes'].append(row.option_code_value)
                else:
                    # First result for this question code, initialize its map/dict details
                    self.question_code_map[row.question_code_value] = {
                        'question_type': SurveyQuestionType(row.question_type),
                        'option_codes': [row.option_code_value, ] if row.option_code_value else None
                    }
                last_question_code_value = row.question_code_value
            self.set_survey_import_time(import_time)

        # Build the header row for the CSV export file, based on survey code/option  lists just created
        self.redcap_header_row.append('record_id')
        for field_name, field_details in self.question_code_map.items():
            if field_details['question_type'] == SurveyQuestionType.CHECKBOX:
                for code in field_details['option_codes']:
                    # Multi-select checkbox questions result in related field names associated with the same parent
                    # question code, for each possible answer selection.  E.g.: sdoh_eds_follow_up_1___sdoh_29,
                    # sdoh_eds_follow_up_1___sdoh_30, etc.  These take on 0 or 1 values in the export data depending
                    # on whether the user checked the associated box for that answer option.
                    self.redcap_header_row.append(self.get_redcap_fieldname(code, parent=field_name))
            else:
                # This should cover TEXT and RADIO (single select) question codes, which become the field name
                # TEXT fields will have empty string/null values in the export data, RADIO fields have the selected
                # option code as their value (or null/empty value if the question was skipped)
                self.redcap_header_row.append(self.get_redcap_fieldname(field_name, parent=None))

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

        row_data = []
        for field_name in self.redcap_header_row:
            if field_name == 'record_id':
                row_data.append(response_id)
            elif field_name in generated_redcap_dict.keys():
                row_data.append(generated_redcap_dict[field_name])
                # Delete recognized keys from the generated REDCap data dict after processing.  Any dict entries left
                # after finishing this for loop is a "non-conformant" field name not in the REDCap data dictionary
                del generated_redcap_dict[field_name]
            else:
                _logger.error(f'Field {field_name} in REDCap definition, not in questionnaire response {response_id}')
                row_data.append(None)

        # Make note of any non-conformant fields from the RDR response data, that REDCap wouldn't recognize
        for unknown_field in generated_redcap_dict.keys():
            _logger.error(f'Field {unknown_field} in questionnaire response {response_id} but not in REDCap definition')

        self.redcap_export_rows.append(row_data)
        return

    def get_module_response_dict(self, module, pid, ro_session):
        """
        Get a PDR module data record (prepped for BigQuery/PostgreSQL) for the specified questionnaire_response_id
        TODO:  If/when the PDR generators are decoupled from RDR, then convert this to query all the
        questionnaire_response_answer data for the response_id.  May mean extending the ResponseValidator class code?
        """
        if not module or not pid:
            print('Need module name and a participant_id')
            return None
        elif ro_session:
            mod_bqgen = BQPDRQuestionnaireResponseGenerator()
            # The table name is returned as the first output, but is unused here (keeping pylint happy by using _)
            _, mod_bqrs = mod_bqgen.make_bqrecord(pid, self.bq_table.get_schema().get_module_name(), latest=True)
            if len(mod_bqrs) > 1:
                # Specifying "latest" should have prevented multiple responses from being returned.
                _logger.error(f'Multiple {self.module} responses returned for participant {pid}')

            response = mod_bqrs[0].to_dict()
            return response
        else:
            pass

        return None

    def map_response_to_redcap_dict(self, response_dict):
        """
        Take the survey response data already generated for PDR and apply its values to the REDCap record export
        PDR has module tables with a column for each question code, where the value is an answer code or comma-separated
        list of answer codes (or for free text fields a 0 or 1 to indicate if text was present)
        """
        redcap_fields = OrderedDict()

        meta_items = dict()
        for key in self.pdr_meta_data_keys:
            meta_items[key] = response_dict[key]
            del response_dict[key]
        lc_response_dict = {k.lower(): v for k, v in response_dict.items()}

        for col in self.question_code_map:
            if col in response_dict.keys():
                val = response_dict.get(col, None)
                del response_dict[col]
                del lc_response_dict[col.lower()]
            elif col in lc_response_dict.keys():
                val = lc_response_dict.get(col, None)
            # PDR data can have comma-separated code strings for answers to multi-select questions
            answers = list(str(val).split(',')) if val else []

            survey_code_type = self.question_code_map[col]['question_type']
            if survey_code_type == SurveyQuestionType.TEXT:
                # PDR data has already mapped null/skipped free text fields to 0 if no text was entered,
                # or 1 if text was present
                if not len(answers) or answers[0] == "0":
                    redcap_fields[self.get_redcap_fieldname(col)] = None
                elif answers[0] == "1":
                    # Use empty string vs. None/Null for REDCap import if text was present.  Putting in other text
                    # like (redacted) caused REDCap import issues.
                    redcap_fields[self.get_redcap_fieldname(col)] = ''
                else:
                    # "Should never get here" but in case there was an issue with PDR data generation....want the
                    # REDCap import process to flag these cases anyway.
                    redcap_fields[self.get_redcap_fieldname(col)] = answers[0]

            elif survey_code_type == SurveyQuestionType.RADIO:
                # Start with a default of None (answers data is empty or only has PMI_Skip)
                redcap_fields[col.lower()] = None
                if len(answers) > 1:
                    # Intentionally generate an "unsplit" string with all the answers.  Want REDCap validation to flag
                    # the answer as invalid (can't be imported into REDCap)
                    redcap_fields[col.lower()] = ','.join(answers)
                    qr_id = response_dict.get('questionnaire_response_id', None)
                    _logger.error(f'Multiple selections found for radio button question {col} (response {qr_id})')
                elif len(answers) and answers[0] != PMI_SKIP_CODE:
                    # REDCap doesn't have anything for "Skip" responses, so only assign a value for other codes
                    redcap_fields[col.lower()] = answers[0]

            elif survey_code_type == SurveyQuestionType.CHECKBOX:
                for option in self.question_code_map[col]['option_codes']:
                    # Ex. of field name assembled here:   sdoh_eds_follow_up_1___sdoh_29
                    # Value is 1 if that option/checkbox was checked (exists in the answers data), 0 otherwise
                    field_name = self.get_redcap_fieldname(option, parent=col)
                    # field_name = "___".join([col, option.lower()])
                    redcap_fields[field_name] = int(option in answers)

        qr_id = meta_items.get('questionnaire_response_id')
        self.add_redcap_export_row(qr_id, redcap_fields)
        unmapped_keys = response_dict.keys()
        if len(unmapped_keys):
            msg = f'Questionnaire response {qr_id} contains codes/answers not in REDCap survey definition:\n'
            for key in unmapped_keys:
                msg += f'\t{key}:  {response_dict[key]}\n'
            _logger.error(msg)

    def export_redcap_csv(self):
        """
        Write the generated REDCap export rows to a file or set of files.  Per REDCap team, best to limit each file to
        1K records since larger exports (e.g., 5K records) were causing timeouts during the REDCap import.
        """
        date_str = datetime.today().strftime('%Y-%m-%d')
        base_name = f'{self.module}_redcap_export_{date_str}_'
        file_number = 1

        for output_rows in list_chunks(self.redcap_export_rows, chunk_size=1000):
            file_name = f'{base_name}_{str(file_number)}.csv'
            print(f'Outputting results to {file_name}...')
            with open(file_name, 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=',')
                csv_writer.writerow(self.redcap_header_row)
                for row in output_rows:
                    csv_writer.writerow(row)
            file_number += 1

    def execute(self):
        """ Run the survey-to-redcap export conversion tool """

        self.gcp_env.activate_sql_proxy(replica=True)
        clr = self.gcp_env.terminal_colors
        _logger.info('')

        _logger.info(clr.fmt('\nExport RDR survey response data for REDCap import:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Module       : {0}'.format(clr.fmt(self.module)))
        _logger.info('  Total PIDS/responses: {0}'.format(clr.fmt(self.num_pids)))
        _logger.info('=' * 90)

        dao = BigQuerySyncDao()
        self.set_bq_table(self.module)
        self.create_survey_code_map(self.module)
        processed = 0
        with dao.session() as session:
            if not self.pid_list:
                self._generate_pid_list()
            for pid in self.pid_list:
                rsp = self.get_module_response_dict(self.module, pid, session)
                if rsp:
                    self.map_response_to_redcap_dict(rsp)
                processed += 1
                if not self.args.debug:
                    print_progress_bar(
                        processed, self.num_pids, prefix="{0}/{1}:".format(processed, self.num_pids), suffix="complete"
                    )

        self.export_redcap_csv()

        return 0

def get_id_list(fname):
    """
    Shared helper routine for tool classes that allow input from a file of integer ids (participant ids or
    id values from a specific table).
    :param fname:  The filename passed with the --from-file argument
    :return: A list of integers, or None on missing/empty fname
    """
    filename = os.path.expanduser(fname)
    if not os.path.exists(filename):
        _logger.error(f"File '{fname}' not found.")
        return None

    # read ids from file.
    ids = open(os.path.expanduser(fname)).readlines()
    # convert ids from a list of strings to a list of integers.
    ids = [int(i) for i in ids if i.strip()]
    return ids if len(ids) else None

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
    parser.add_argument("--module", help="Module name for data export (e.g., TheBasics)", type=str, default='sdoh')
    parser.add_argument("--number", help="The number of random participants whose module responses should be exported",
                        type=int, default=25)
    parser.add_argument("--from-file", help="file with participant ids whose module data will be exported",
                         metavar='FILE', type=str, default=None)
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        ids = None
        if hasattr(args, 'from_file') and args.from_file:
            ids = get_id_list(args.from_file)

        process = SurveyToRedCapConversion(args, gcp_env, pid_list=ids)
        exit_code = process.execute()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
