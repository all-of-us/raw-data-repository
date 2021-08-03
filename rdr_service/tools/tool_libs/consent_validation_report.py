#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys
import os
import gspread
import pandas
from time import sleep
from datetime import datetime
from gspread.utils import rowcol_to_a1
from gspread.exceptions import APIError

from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.services.gcp_utils import gcp_get_iam_service_key_info
from rdr_service.model.consent_file import ConsentSyncStatus, ConsentType

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "consent-report"
tool_desc = "Publish consent validation metrics to a google sheet document"

# This list matches the names of the column names / calculated fields returned from the DAILY_REPORT_SQL query
# NOTE:  Errors added to this list should also have an entry in the DAILY_REPORT_COLUMN_MAP for enabling reporting
TRACKED_CONSENT_ERRORS = [
    'missing_file',
    'signature_missing',
    'invalid_signing_date',
    'invalid_dob',
    'invalid_age_at_consent',
    'checkbox_unchecked',
    'non_va_consent_for_va',
    'va_consent_for_non_va'
]

# 1-based Spreadsheet Column positions for the pieces of information to be added during generation
DAILY_REPORT_COLUMN_MAP = {

    'banner': 1,   # Banner text, e.g., Report Date or No consent errors detected text written to Column A
    'hpo': 1, # Col A HPO or Organization name string
    'organization': 1,
    # Column B intentionally left blank;  Summary counts are in columns C-F:
    'consent_type': 3,
    'expected': 4,
    'ready_to_sync': 5,
    'total_errors': 6,
    # -- Keys for each of the error conditions defined in the TRACKED_CONSENT_ERRORS list.  Column list may grow
    'missing_file': 7,    # Column G
    'signature_missing': 8,
    'invalid_signing_date': 9,
    'invalid_dob': 10,
    'invalid_age_at_consent': 11,
    'checkbox_unchecked': 12,
    'non_va_consent_for_va': 13,
    'va_consent_for_non_va': 14   # Column N
}

# Iterable list of the ConsentType enum ints (used to compare against DB consent_file.type field values)
CONSENTS_LIST = [int(v) for v in ConsentType]

class ProgramTemplateClass(object):

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().

        Other attributes:
        doc_id:   Google UID for the doc (documentId in doc URL: https://docs.google.com/document/d/documentId/edit)
                  Can be passed to the tool via --doc-id parameter, or read from CONSENT_DOC_ID environment variable
                  Eventually, it will be defined in the app config items
        worksheet:  Returned from gspread add_worksheet() when a sheet is added to the gsheets doc
        daily_data:  A pandas dataframe of the results from the DAILY_REPORT_SQL query
        consent_errors_found:  True/False if the daily_data contains NEEDS_CORRECTING consent records
        report_date:   Date of the consent validation run (created date for records in RDR consent_file table)
        dob_date_cutoff:  report_date-125 years (will flag DOB values more than 125 years ago as invalid)
        sheet_rows:  Max rows for the sheet being created (arbitrary, trying to accommodate expected "worst case")
        sheet_cols:  Max cols, derived from the DAILY_REPORT_COLUMN_MAP values
        max_retries:  How many times to retry a sheet write operation if the gspread/gsheets API call fails
        max_daily_reports:  How many tabs/sheets to keep in the spreadsheet file before deleting the oldest
        row_pos:  Keeps track of the current row position in the spreadsheet, as content is written
        row_layout:  A dict with known values and formatting options of report sections, passed to gspread/gsheets API
                     The keys are arbitrary names given to each of the known sections/content areas in the daily report
        """
        self.args = args
        self.gcp_env = gcp_env

        if args.doc_id:
            self.doc_id = args.doc_id
        else:
            # TODO:  If doc_id was not passed in, get it from environment var for now.  Update to read it from config
            self.doc_id = os.environ['CONSENT_DOC_ID']

        if not self.doc_id:
            raise ValueError('Please use the --doc-id arg or export CONSENT_DOC_ID environment var')


        self.worksheet = None   # Set to the newly created worksheet from gspread add_worksheet()
        self.daily_data = None  # Set to the pandas dataframe result generated from the DAILY_REPORT_SQL query
        self.consent_errors_found = False  # Set to True if daily_data dataframe contains NEEDS_CORRECTING values

        if args.report_date:
            self.report_date = args.report_date
        else:
            # Default to today's date
            self.report_date = datetime.now()

        # Decision by DRC/NIH stakeholders to use 125 years ago as the cutoff date for flagging invalid DOB
        self.dob_date_cutoff = datetime(self.report_date.year-125,
                                        self.report_date.month,
                                        self.report_date.day).strftime("%Y-%m-%d")

        # Max dimensions for the daily sheet (max rows is a guesstimate?)
        self.sheet_rows = 500
        self.sheet_cols = max(DAILY_REPORT_COLUMN_MAP.values())

        # Retry limit if a gspread/sheets API request fails
        self.max_retries = 3

        # Number of days/worksheets to archive in the file (will do rolling deletion of oldest daily worksheets/tabs)
        self.max_daily_reports = 30

        # A row position tracker updated as content is added to the daily report worksheet
        self.row_pos = 1

        # Pre-populated details about the sections of the daily report;  used when calling gspread methods
        self.row_layout = {
            # The banner at the top of each report, e.g.: Report Date: Jul 22, 2021 (generated
            'report_date': {
                'values': ['Report for Date: ' + self.report_date.strftime("%b %-d, %Y") + \
                           ' (generated on {} Central)'.format(datetime.now().strftime("%c"))],
                'format': {'textFormat': {'fontSize': 12, 'bold': True}}
            },
            # This text only appears if there were no validation errors that day
            'no_errors': {
                'values': ['No consent validation errors detected'],
                'format': {'textFormat': {'fontSize': 12, 'italic': True}}
            },
            # The banner text that precedes the summarized counts (across all hpos/orgs) for the day
            'total_counts': {
                'values': ['Total Consent Validation Counts'],
                'format': {'textFormat': {'bold': True}}
            },
            # The banner text that precedes the start of the HPO-specific counts sections (only if errors exist)
            'counts_by_org': {
                'values': ['Counts by HPO/Organization (for entities having one or more consent errors)'],
                'format': {'textFormat': {'bold': True}}
            },
            # The shaded header row that we insert with all the column headers.  Precedes the total counts summary
            # section, and then appears once for every HPO for which there were associated consent errors
            # The first item in the values list will be dynamically updated with the appropriate string/HPO name
            'count_section': {
                'values': ['','',
                           'Consent Type',
                           'Expected',
                           'Ready to Sync',
                           'Total Errors',
                           'Missing File',
                           'Signature Missing',
                           'Signature Date Mismatch',
                           'Invalid DOB',
                           'Age at Primary Consent <18',
                           'Checkbox Unchecked',
                           'Non-VA Consent for VA Participant',
                           'VA Consent for Non-VA Participant'],
                'format': {'textFormat': {'bold': True},
                           'wrapStrategy': 'WRAP',
                           'borders': {'top': {'style': 'SOLID'},'bottom': {'style': 'SOLID'}},
                           'backgroundColor': {"red": 0.02, "green": 0.8, "blue": 0.4},
                           'verticalAlignment': 'MIDDLE'
                           }

            },
        }

    def get_daily_consent_validation_results(self, db_conn=None):
        """
        Queries the RDR consent_file table and populates the pandas DataFrame with the validation results
        from the specified report date (records with matching created date).   Sets self.daily_data to the dataframe

        Notes on the DAILY_REPORT_SQL logic, to prevent flagging errors that should only be counted when there wasn't
        a higher-level error related to the consent:
        - A signature_missing error will only be true when the consent file exists
        - An invalid_signing_date error will only be true when the signature is valid (exists)
        """
        if not db_conn:
            raise(EnvironmentError, 'No active DB connection object')

        DAILY_REPORT_SQL = """
            SELECT cf.participant_id,
                   p.date_of_birth,
                   -- age_at_consent is specific/relevant to PRIMARY consent type (uses its authored date)
                   CASE WHEN cf.type = 1 THEN
                        CASE WHEN (p.date_of_birth IS NULL or p.consent_for_study_enrollment_authored IS NULL) THEN NULL
                             ELSE TIMESTAMPDIFF(YEAR, p.date_of_birth, p.consent_for_study_enrollment_authored)
                        END
                   END AS age_at_consent,
                   CASE
                      WHEN (h.name IS NOT NULL and h.name != 'UNSET') THEN h.name
                      ELSE '(No HPO details)'
                   END AS hpo,
                   CASE
                      WHEN o.display_name IS NOT NULL THEN o.display_name ELSE '(No organization details)'
                   END AS organization,
                   cf.sync_status,
                   cf.type,
                   cf.file_path,
                   -- CASE Statements to calculate the known tracked error conditions
                   CASE WHEN cf.file_exists = 0 THEN 1 ELSE 0 END AS missing_file,
                   CASE WHEN (cf.file_exists and cf.is_signature_valid = 0) THEN 1 ELSE 0 END AS signature_missing,
                   CASE WHEN (cf.is_signature_valid and cf.is_signing_date_valid = 0) THEN 1 ELSE 0
                   END AS invalid_signing_date,
                   CASE WHEN (p.date_of_birth is null or p.date_of_birth < "{}" or p.date_of_birth > "{}"
                              or (p.consent_for_study_enrollment_authored is not null
                                  and p.date_of_birth > p.consent_for_study_enrollment_authored ) )
                        THEN 1 ELSE 0
                   END AS invalid_dob,
                   CASE WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, p.consent_for_study_enrollment_authored) < 18
                        THEN 1 ELSE 0
                   END AS invalid_age_at_consent,
                   CASE
                   -- Map the text for other errors we know about to its TRACKED_CONSENT_ERRORS name
                     WHEN (cf.file_exists AND cf.other_errors LIKE '%missing consent check mark%')
                     THEN 1 ELSE 0
                   END AS checkbox_unchecked,
                   CASE
                     WHEN (cf.file_exists AND cf.other_errors LIKE '%non-veteran consent for veteran participant%')
                     THEN 1 ELSE 0
                   END AS non_va_consent_for_va,
                   CASE
                     WHEN (cf.file_exists AND cf.other_errors LIKE '%veteran consent for non-veteran participant%')
                     THEN 1 ELSE 0
                   END AS va_consent_for_non_va
            FROM consent_file cf
            JOIN participant_summary p on p.participant_id = cf.participant_id
            LEFT OUTER JOIN hpo h on p.hpo_id = h.hpo_id
            LEFT OUTER JOIN organization o on p.organization_id = o.organization_id
            -- Limit the pulled records to those in either a NEEDS_CORRECTING, READY_TO_SYNC, or SYNC_COMPLETED status
            -- The other statuses are not relevant to the daily report metrics
            WHERE DATE(cf.created) = "{}" AND cf.sync_status IN (1, 2, 4)
            -- TODO:   DELETE THIS CLAUSE AFTER CE file validations are implemented
            AND p.participant_origin = 'vibrent'
        """

        # DAILY_REPORT_SQL string has three placeholders to populate:
        # dob_date_cutoff and the report_date are used for validating DOB, and report_date is also used in the
        # WHERE clause to filter on records created on that date.
        report_date_filter = self.report_date.strftime("%Y-%m-%d")
        sql = DAILY_REPORT_SQL.format(self.dob_date_cutoff, report_date_filter, report_date_filter)

        # Load daily validation results into a pandas dataframe and save as instance variable
        df = self.daily_data = pandas.read_sql_query(sql, db_conn)

        # NOTE: For testing w/o hitting the prod DB:  can comment out the pandas.read_sql_query statement and use a
        # saved off CSV results file instead, e.g.:
        # df = self.daily_data = pandas.read_csv('20210722_consents.csv')

        # Pandas: filter all NEEDS_CORRECTING rows in the dataframe; shape[0] is resulting row count
        self.consent_errors_found = df.loc[(df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING))].shape[0] > 0

        # TODO:  Remove after development is complete; for quick confirmation via console output if there were errors
        if self.consent_errors_found:
            _logger.info(df.loc[(df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING))])

    def write_to_worksheet(self, cell_range, values, format_specs=None):
        """
        A helper routine that will perform the worksheet write operations, especially until this tool can be updated to
        use gspread/gsheets batch update requests.

        There's a rate limit of gsheets API requests per minute, so if there are an unusually large number of validation
        validation records across many organizations, we can exceed the limit by doing unbatched writes.
        """
        formatting_complete = False
        write_complete = False
        i = 0
        while not write_complete and i < self.max_retries:
            try:
                # Do the formatting first so if it triggers retry, we don't write the cell data again on next attempt
                if format_specs and not formatting_complete:
                    self.worksheet.format(cell_range, format_specs)
                    formatting_complete = True
                self.worksheet.update(cell_range, [values])
                write_complete = True
            except APIError:
                _logger.info('Pausing 60 seconds to stay within gsheets request rate limit...')
                sleep(60)
                _logger.info('Resuming....')
            finally:
                i += 1

    def add_banner_text_row(self, banner_key, row_pos=None):
        """
          Add a row with the requested banner text (e.g., Report Date line)
          Gets the values and formatting information from the instance row_layout dictionary
        """
        if not row_pos:
            row_pos = self.row_pos

        banner = self.row_layout.get(banner_key)
        if banner:
            # Banner text rows only have one cell of text to populate in column A; E.g., cell_range = 'A1' or 'A3', etc.
            cell_range = rowcol_to_a1(row_pos, 1)
            self.write_to_worksheet(cell_range, banner.get('values'), format_specs=banner.get('format'))
            self.row_pos = row_pos + 1

    def add_count_header_section(self, row_pos=None, hpo=None):
        """
        Builds a counts section shaded header row  with the report column headers (Expected, Ready to Sync, etc.)
        """
        if not row_pos:
            row_pos = self.row_pos

        section = self.row_layout.get('count_section')
        # Replace the first col empty value in the the pre-populated values list with an HPO name, if provided
        if hpo:
            section['values'][0] = hpo
        # E.g. cell_range = 'A5:N5' for row 5
        cell_range = rowcol_to_a1(row_pos, 1) + ':' + rowcol_to_a1(row_pos, self.sheet_cols)
        self.write_to_worksheet(cell_range, section.get('values'), format_specs=section.get('format'))
        self.row_pos = row_pos + 1


    def add_consent_counts(self, df, row_pos=None, org=None):
        """
          Builds and populates a subsection of rows, with one row per consent type, indicating its status/error counts
          This method can be called on either the full daily data unfiltered dataframe for the daily total summary
          counts, or an Organization-specific dataframe for its error counts
        """
        if not row_pos:
            row_pos = self.row_pos

        # Using integer ConsentType enum values to compare against consent_file.type (df.type) values
        for consent in CONSENTS_LIST:

            # Organization name string gets written once to column A only in the first pass through this loop
            # E.g., cell_range = 'A7' for row_pos 7
            if org and consent == CONSENTS_LIST[0]:
                cell_range = rowcol_to_a1(row_pos, 1)
                self.write_to_worksheet(cell_range, [org],
                                        format_specs={'textFormat': {'bold': True}, 'wrapStrategy': 'wrap' })

            # Skip row creation if no consents of this type were processed that day.  Otherwise, we'll print a summary
            # for each consent type in this organization's list of processed consents, regardless of whether that
            # consent was the one with the errors.
            expected = df.loc[(df.type == consent)].shape[0]
            if not expected:
                continue

            ready = df.loc[(df.type == consent)\
                           & (df.sync_status != int(ConsentSyncStatus.NEEDS_CORRECTING))].shape[0]
            errors = df.loc[(df.type == consent)\
                            & (df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING))].shape[0]

            # Partial row population for the summary counts column cells.  E.g., cell_range = 'C7:F7' for row_pos 7
            cell_range = rowcol_to_a1(row_pos, DAILY_REPORT_COLUMN_MAP.get('consent_type')) + ':' + \
                         rowcol_to_a1(row_pos, DAILY_REPORT_COLUMN_MAP.get('total_errors'))
            consent_summary_values =[str(ConsentType(consent)), expected, ready, errors]
            self.write_to_worksheet(cell_range, consent_summary_values)

            if errors:
                # This consent had an error count.   Calculate which error(s) and how many, and build a list of
                # values to populate the error-specific columns in this consent's row
                tracked_error_values = []
                for error in TRACKED_CONSENT_ERRORS:
                    count = df.loc[(df.type == consent)][error].sum()
                    if count:
                        tracked_error_values.append(int(count))   # Cast sum() int64 dtype result to int for gsheets?
                    else:
                        tracked_error_values.append(None)

                # Partial row write for the individual error counts columns E.g. cell_range = 'G7:N7' for row_pos 7
                cell_range = rowcol_to_a1(row_pos, DAILY_REPORT_COLUMN_MAP.get(TRACKED_CONSENT_ERRORS[0])) + ':' + \
                        rowcol_to_a1(row_pos, DAILY_REPORT_COLUMN_MAP.get(TRACKED_CONSENT_ERRORS[-1]))

                self.write_to_worksheet(cell_range, tracked_error_values)

            row_pos += 1

        self.row_pos = row_pos + 1

    def add_daily_errors(self):
        """"
        This method is called when there is a daily error count.   It will generate HPO/Organization-specific
        breakdowns of the consent error metrics.  Only organizations for which there were associated errors will
        be included in the report output.
        """
        self.add_banner_text_row('counts_by_org', row_pos=self.row_pos + 1)

        # Iterate through list of distinct HPOs found in the full daily results dataframe.  May include UNSET HPO
        # value, mapped to string '(No HPO details)' by the DAILY_REPORT_SQL query
        hpos = self.daily_data['hpo'].unique()
        for hpo in sorted(hpos):
            hpo_df = self.daily_data[self.daily_data.hpo == hpo]   # Yields an HPO-filtered dataframe

            # If any rec associated with this HPO has consents with errors,  create HPO block section header
            if hpo_df.loc[(hpo_df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING))].shape[0]:
                self.add_count_header_section(hpo=hpo)
            else:
                continue

            # Now iterate over distinct organizations in the HPO-specific dataframe to build subsections for
            # any organization in that HPO that had the consent errors.  May include UNSET/null organization, which
            # is mapped to '(No organization details)' by DAILY_REPORT_SQL query
            orgs = hpo_df['organization'].unique()
            for org in sorted(orgs):
                org_df = hpo_df[hpo_df.organization == org]   # Yields an Org-filtered dataframe from the HPO frame

                # Add breakdown of consent and error counts for any organization that had any NEEDS_CORRECTING consents
                if org_df.loc[(org_df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING))].shape[0]:
                    self.add_consent_counts(org_df, row_pos=self.row_pos + 1, org=org)
                else:
                    continue

    def add_daily_summary(self):
        """ Add content that appears on every daily consent validation report regardless of errors """
        self.add_banner_text_row('report_date')

        if not self.consent_errors_found:
            self.add_banner_text_row('no_errors', row_pos=self.row_pos + 1)
        self.add_banner_text_row('total_counts', row_pos=self.row_pos + 1)

        # Daily summary counts for all the consents that were processed (regardless of whether errors were detected)
        self.add_count_header_section(hpo='All Entities')
        self.add_consent_counts(self.daily_data)


    def create_daily_report(self, spreadsheet):
        """
        Add a new daily report tab/sheet to the google sheet file, with the validation details for that date
        """
        existing_sheets = spreadsheet.worksheets()
        # Perform rolling deletion of the oldest reports so we keep a pre-defined maximum number of daily reports
        # NOTE:  this assumes all the reports in the file were generated in order, with the most recent date at the
        # leftmost tab (index 0).   This truncates the rightmost tabs
        for ws_index in range(len(existing_sheets), self.max_daily_reports-1, -1):
            spreadsheet.del_worksheet(existing_sheets[ws_index-1])

        # Add the new worksheet (to leftmost tab position / index 0)
        self.worksheet = spreadsheet.add_worksheet(self.report_date.strftime("%b %d %Y"),
                                                   rows=self.sheet_rows, cols=self.sheet_cols, index=0)
        self.add_daily_summary()
        if self.consent_errors_found:
            # Google sheets doesn't have flexible/multiple freezing options.  Freeze all rows above the current position
            # so HPO/Org-specific section(s) are scrollable while still seeing column header names from Total Counts
            self.worksheet.freeze(rows=self.row_pos - 1)
            self.add_daily_errors()

        _logger.info('Report complete')


    def execute(self):
        """
        Execute the consent report builder.  Currently only handles the daily consent validation report, but could
        be extended so the same tool can handle multiple report types based on a user-provided argument
        """

        # Set up DB and googlesheets doc access
        self.gcp_env.activate_sql_proxy()
        db_conn = self.gcp_env.make_mysqldb_connection()
        service_key_info = gcp_get_iam_service_key_info(self.gcp_env.service_key_id)
        gs_creds = gspread.service_account(service_key_info['key_path'])
        gs_file = gs_creds.open_by_key(self.doc_id)

        # Build the report
        self.get_daily_consent_validation_results(db_conn=db_conn)
        self.create_daily_report(gs_file)



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
    # TODO:  Replace CONSENT_DOC_ID environment variable with reading the doc ID value from the config settings
    parser.add_argument("--doc-id", help="A google sheet ID which can override a CONSENT_DOC_ID env var")
    parser.add_argument("--report-date", type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help="Date of the consent validation job in YYYY-MM-DD format.  Default is today's date")
    args = parser.parse_args()


    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ProgramTemplateClass(args, gcp_env)
        exit_code = process.execute()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
