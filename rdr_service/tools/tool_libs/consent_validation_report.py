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
import csv
import gspread
import pandas
from time import sleep
from datetime import datetime, timedelta
from gspread.utils import rowcol_to_a1
from gspread.exceptions import APIError

from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.services.gcp_config import RdrEnvironment
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.services.gcp_utils import gcp_get_iam_service_key_info
from rdr_service.model.consent_file import ConsentSyncStatus, ConsentType

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "consent-report"
tool_desc = "Publish consent validation metrics to a google sheets doc and/or create a CSV with consent error details"

# This list matches the names of the column names / calculated fields returned from the custom SQL query that pulls
# data from the RDR consent_file table.  NOTE:  Errors added to this list should also have an entry in the
# the DAILY_REPORT_COLUMN_MAP for enabling reporting in the google sheets doc
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

# The PTSC CSV file has some identifying information columns about consents with errors, plus the error status columns
PTSC_CSV_COLUMN_HEADERS = ['participant_id', 'type', 'file_path', 'file_upload_time'] + TRACKED_CONSENT_ERRORS

# 1-based Spreadsheet Column positions for the pieces of information to be added during generation
DAILY_REPORT_COLUMN_MAP = {

    # -- Keys for the summary information columns
    'banner': 1,   # Banner text (e.g., Report Date text) always written to Column A
    'hpo': 1,      # Also use column A for HPO or Organization name strings
    'organization': 1,
    # Column B intentionally left blank
    'consent_type': 3, # Column C
    'expected': 4,
    'ready_to_sync': 5,
    'consents_with_errors': 6,
    'total_errors': 7,   # Column G
    # -- Keys for each of the error conditions defined in the TRACKED_CONSENT_ERRORS list.  Column list may grow
    'missing_file': 8,    # Column H
    'signature_missing': 9,
    'invalid_signing_date': 10,
    'invalid_dob': 11,
    'invalid_age_at_consent': 12,
    'checkbox_unchecked': 13,
    'non_va_consent_for_va': 14,
    'va_consent_for_non_va': 15   # Column O
}

# TODO:  Convert to SQLAlchemy when refactoring for automation.  Raw SQL used initially for fast prototyping of reports
CONSENT_REPORT_SQL_BODY =  """
            SELECT cf.participant_id,
                   ps.date_of_birth,
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
                   cf.file_upload_time,
                   -- Calculated fields to generate 0 or 1 values for the known tracked error conditions
                   -- (1 if error found)
                   NOT cf.file_exists AS missing_file,
                   (cf.file_exists and NOT is_signature_valid) AS signature_missing,
                   (cf.is_signature_valid and NOT cf.is_signing_date_valid) AS invalid_signing_date,
                   -- Invalid DOB conditions: DOB missing, DOB before defined cutoff, DOB in the future, or
                   -- DOB later than the consent authored date
                   (ps.date_of_birth is null or ps.date_of_birth < "{dob_cutoff}" or ps.date_of_birth > "{report_date}")
                    or (ps.consent_for_study_enrollment_authored is not null
                        and ps.date_of_birth > ps.consent_for_study_enrollment_first_yes_authored )
                    AS invalid_dob,
                   TIMESTAMPDIFF(YEAR, COALESCE(ps.date_of_birth, CURRENT_DATE),
                                 ps.consent_for_study_enrollment_first_yes_authored) < 18
                    AS invalid_age_at_consent,
                   -- Map the text for other errors we know about to its TRACKED_CONSENT_ERRORS name
                   (cf.file_exists AND cf.other_errors LIKE '%missing consent check mark%') AS checkbox_unchecked,
                   (cf.file_exists AND cf.other_errors LIKE '%non-veteran consent for veteran participant%')
                      AS non_va_consent_for_va,
                   (cf.file_exists AND cf.other_errors LIKE '%veteran consent for non-veteran participant%')
                      AS va_consent_for_non_va
            FROM participant_summary ps
            -- Eliminate test/ghost pids from the result
            JOIN participant p on p.participant_id = ps.participant_id
                 AND p.is_test_participant = 0 and (p.is_ghost_id is null or not p.is_ghost_id) and p.hpo_id != 21
            JOIN consent_file cf ON ps.participant_id = cf.participant_id
            LEFT OUTER JOIN hpo h ON p.hpo_id = h.hpo_id
            LEFT OUTER JOIN organization o on ps.organization_id = o.organization_id
        """

# Daily report filter for validation results on all newly received and validated consents:
# - For each consent type, filter on participants whose consent authored date for that consent matches the report date
# - Find corresponding consent_file entries for the consent type, in NEEDS_CORRECTING/READY_TO_SYNC/SYNC_COMPLETE
DAILY_NEW_CONSENTS_SQL_FILTER = """
            WHERE cf.type = {consent_type}
                  AND DATE(ps.{authored_field}) = "{report_date}"
                  AND cf.sync_status IN (1,2,4)
    """

# Filter to produce a report of all remaining NEEDS_CORRECTING consents, regardless of date
ALL_UNRESOLVED_ERRORS_SQL_FILTER = 'WHERE cf.sync_status = 1 '

# TODO:  Remove this when we expand consent validation to include CE consents
VIBRENT_SQL_FILTER = ' AND ps.participant_origin = "vibrent"'


# Define the allowable --report-type arguments and their associated SQL.
REPORT_TYPES = {
    # Daily uploads = validation for all consents authored on the report date + missing files flagged on the report date
    'daily_uploads':  CONSENT_REPORT_SQL_BODY + DAILY_NEW_CONSENTS_SQL_FILTER + VIBRENT_SQL_FILTER,
    # Unresolved errors = Any consent_file entries still in a NEEDS_CORRECTING state (all-time)
    'unresolved_errors':  CONSENT_REPORT_SQL_BODY + ALL_UNRESOLVED_ERRORS_SQL_FILTER + VIBRENT_SQL_FILTER
}

# Maps the currently validated consent types to the authored date field that will be used in the report SQL query
CONSENT_AUTHORED_FIELDS = {
    # For PRIMARY:  use earliest consent authored (to distinguish from PrimaryConsentUpdate authored, which are not
    # yet included in the validation)
    ConsentType.PRIMARY : 'consent_for_study_enrollment_first_yes_authored',
    ConsentType.CABOR: 'consent_for_cabor_authored',
    ConsentType.EHR: 'consent_for_electronic_health_records_authored',
    ConsentType.GROR: 'consent_for_genomics_ror_authored',
}

# List of currently validated consent type values as ints, for pandas filtering of consent_file.type values
CONSENTS_LIST = [int(v) for v in CONSENT_AUTHORED_FIELDS.keys()]

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
        consent_df:  A pandas dataframe of the results from the report SQL query
        consent_errors_found:  True/False if the consent_df contains NEEDS_CORRECTING consent records
        report_date:   Date of the consent validation run (created date for records in RDR consent_file table)
        dob_date_cutoff:  report_date-125 years (will flag DOB values more than 125 years ago as invalid)
        sheet_rows:  Max rows for the sheet being created (arbitrary, trying to accommodate expected "worst case")
        sheet_cols:  Max cols, derived from the DAILY_REPORT_COLUMN_MAP values
        max_retries:  How many times to retry a sheet write operation if the gspread/gsheets API call fails
        max_daily_reports:  How many tabs/sheets to keep in the spreadsheet file before deleting the oldest
        row_pos:  Keeps track of the current row position in the spreadsheet, as content is written
        write_requests:  Tracks number of gsheets API requests, to throttle for rate limit restrictions
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
        self.consent_df = None  # Set to the pandas dataframe result generated from the report SQL query
        self.consent_errors_found = False  # Set to True if consent_df dataframe contains NEEDS_CORRECTING values

        if args.report_date:
            self.report_date = args.report_date
        else:
            # Default to yesterday's date as the filter for consent authored date
            self.report_date = datetime.now() - timedelta(1)

        if args.csv_file:
            self.csv_filename = args.csv_file
        else:
            self.csv_filename = f'{self.report_date.strftime("%Y%m%d")}_consent_errors.csv'

        # Decision by DRC/NIH stakeholders to use 125 years ago as the cutoff date for flagging invalid DOB
        self.dob_date_cutoff = datetime(self.report_date.year-125,
                                        self.report_date.month,
                                        self.report_date.day).strftime("%Y-%m-%d")

        if args.report_type:
            if not args.report_type in REPORT_TYPES.keys():
                raise ValueError(f'invalid report type option: {args.report_type}')
            else:
                self.report_sql = REPORT_TYPES.get(args.report_type)

        # Max dimensions for the daily sheet (max rows is a guesstimate?)
        self.sheet_rows = 500
        self.sheet_cols = max(DAILY_REPORT_COLUMN_MAP.values())

        # Retry limit if a gspread/sheets API request fails
        self.max_retries = 3

        # Number of days/worksheets to archive in the file (will do rolling deletion of oldest daily worksheets/tabs)
        self.max_daily_reports = 32 # A month's worth + an extra sheet to contain a legend / notes as needed

        # Trackers updated as content is added to the daily report worksheet
        self.row_pos = 1
        self.write_requests = 0

        # Pre-populated details about the sections of the reports;  used when calling gspread methods
        self.row_layout = {
            # The banner at the top of each report, e.g.: Report Date: Jul 22, 2021 (generated
            'report_date': {
                'values': ['Report for consents authored on: ' + self.report_date.strftime("%b %-d, %Y") + \
                           f' 12:00AM-11:59PM UTC (generated on {datetime.now().strftime("%x %X")} Central)'],
                'format': {'textFormat': {'fontSize': 12, 'bold': True}}
            },
            # Display any additional details of note, such as current limitations of validation tools
            'report-notes': {
                'values': [
                    'Notes:',
                    'Validation is currently only done for PTSC consent files (does not include CareEvolution)',
                    'Checkbox validation currently only performed on GROR consents',
                    'Total Errors can exceed Consents with Errors if any consents had multiple validation errors'
                ],
                'format': {'textFormat':
                               {'fontSize': 10,
                                'italic': True,
                                'foregroundColor': {"red": 0.0, "green": 0.0, "blue": 1.0}
                                }
                           }
            },
            # This text only appears if there were no validation errors that day
            'no_errors': {
                'values': ['No consent validation errors detected'],
                'format': {'textFormat': {'fontSize': 12, 'italic': True}}
            },
            # The banner text that precedes the summarized counts (across all hpos/orgs) for the day
            'total_consent_counts': {
                'values': ['Total Consent Validation Counts'],
                'format': {'textFormat': {'bold': True}}
            },
            # The banner text that precedes the start of the HPO-specific counts sections (only if errors exist)
            'counts_by_org': {
                'values': ['Consent Errors by HPO/Organization'],
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
                           'Consents with Errors',
                           'Total Errors',
                           'Missing File',
                           'Signature Missing',
                           'Signature Date Invalid',
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

    def make_a1_notation(self, start_row, start_col=1, end_row=None, end_col=None):
        """
        Use the rowcol_to_a1() gspread method to construct an A1 cell range notation string
        A starting row position is required.  A starting col of 1 is the presumed default.  If no ending row/col is
        provided, then assume the ending position is the same row and/or column

        Returns:  a string such as 'A1:A1' (single cell), 'A5:N5' (multiple columns on the same row), etc.
        """

        # Assume single row / single col if no ending coordinate is provided
        end_row = end_row or start_row
        end_col = end_col or start_col

        # Sanity check on row and column values vs. defined spreadsheet dimensions
        if start_row > self.sheet_rows or end_row > self.sheet_rows:
            raise ValueError(f'Row value exceeds maximum of {self.sheet_rows}')
        if start_col > self.sheet_cols or end_col > self.sheet_cols:
            raise ValueError(f'Column value exceeds maximum of {self.sheet_cols}')

        return ''.join([rowcol_to_a1(start_row, start_col), ':', rowcol_to_a1(end_row, end_col)])

    def get_daily_consent_validation_results(self, db_conn=None):
        """
        Queries the RDR consent_file table and populates the pandas DataFrame with the validation results
        from the specified report date.   Sets self.consent_df to the dataframe.  The results will also be
        used to populate a CSV file with details on newly flagged errors, sent to PTSC
        """
        if not db_conn:
            raise(EnvironmentError, 'No active DB connection object')

        df = pandas.DataFrame()
        for consent_int in CONSENTS_LIST:
            sql = self.report_sql.format(authored_field=CONSENT_AUTHORED_FIELDS[ConsentType(consent_int)],
                                         consent_type=consent_int,
                                         dob_cutoff=self.dob_date_cutoff,
                                         report_date=self.report_date.strftime("%Y-%m-%d"))

            df = df.append(pandas.read_sql_query(sql, db_conn))

        # NOTE: For testing w/o hitting the prod DB:  can comment out the pandas.read_sql_query statement and use a
        # saved off CSV results file instead, e.g.:
        # df = self.consent_df = pandas.read_csv('20210722_consents.csv')

        # Load daily validation results into a pandas dataframe.  Fill in any null/NaN error count columns with
        # (uint8) 0s
        for error_type in TRACKED_CONSENT_ERRORS:
            df = df.fillna({error_type: 0}).astype({error_type: 'uint8'})

        # Pandas: Row count (shape[0]) of dataframe filtered on NEEDS_CORRECTING > 0 means errors exist
        self.consent_errors_found = df.loc[df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING)].shape[0] > 0

        # Save resulting dataframe to instance variable
        self.consent_df = df

    def create_csv_errors_file(self):
        """
        Generate a CSV file with the consent error details (originally just for PTSC).
        TODO:  May need to simultaneously create two CSV files once we add validations for CE?  Unless API is done?
        """
        errors_df = self.consent_df[self.consent_df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING)]

        # Initialize the list of lists which will be passed to CSV writer writerows(), with the first row of headers
        output_rows = [PTSC_CSV_COLUMN_HEADERS]

        # iterrows() allows us to iterate through the dataframe similar to a result set of records.  It also returns
        # an index, which is unused here and replaced with _ to keep pylint happy
        for _, df_row in errors_df.iterrows():
            # If file_path was null/file was missing, coerce file details to empty strings
            if not df_row['file_path']:
                file_path = file_upload_time = ''
            else:
                file_path = df_row['file_path']
                file_upload_time = df_row['file_upload_time']

            # Generating values at the start of each CSV line such as:
            # P111111111,GROR,ptc-uploads-all-of-us-prod/Participant/P11111111/GROR__000.pdf,2021-08-10 01:38:21,...
            csv_values = [
                'P' + str(df_row['participant_id']),
                str(ConsentType(df_row['type'])),
                file_path,
                file_upload_time
            ]
            # Add the 0/1 values for each of the row's error flag fields
            for error_type in TRACKED_CONSENT_ERRORS:
                csv_values.append(df_row[error_type])

            output_rows.append(csv_values)

        # Write out the csv file to the local directory
        with open(self.csv_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(output_rows)

    def write_to_worksheet(self, cell_range, values, format_specs=None):
        """
        A helper routine that will perform the worksheet write operations, especially until this tool can be updated to
        use gspread/gsheets batch update requests.

        There's a rate limit of gsheets API requests per minute, so if there are an unusually large number of daily
        validation records across many organizations, we can exceed the limit doing the unbatched writes.
        """
        formatting_complete = False
        write_complete = False
        i = 0
        self.write_requests += 1
        # Rate limits are 100 writes per user every 100 seconds;  inject a pause every 25 writes
        if self.write_requests % 25 == 0:
            sleep(25)

        while not write_complete and i < self.max_retries:
            try:
                # Do the formatting first so if it triggers retry, we don't write the cell data again on next pass
                if format_specs and not formatting_complete:
                    self.worksheet.format(cell_range, format_specs)
                    formatting_complete = True

                if len(values):   # May have empty values list if this is to write formatting only
                    self.worksheet.update(cell_range, [values])
                write_complete = True

            except APIError as e:
                if 'RATE_LIMIT_EXCEEDED' in str(e):
                    _logger.info('gsheets rate limit per 100 seconds exceeded, pausing...')
                    sleep(60)
                    _logger.info('Resuming....')
                else:
                    raise e
            finally:
                i += 1

    def add_banner_text_row(self, banner_key, row_pos=None):
        """
          Add a row or rows with the requested banner text (e.g., Report Date line).  A banner section can contain
          multiple lines of text (e.g., 'report-notes' banner key)

          Gets the values and formatting information from the class row_layout dictionary
        """
        if not row_pos:
            row_pos = self.row_pos

        banner = self.row_layout.get(banner_key)
        if banner:
            banner_format = banner.get('format', None)
            for banner_row in banner.get('values', []):
                # Each row of banner text is a single-cell range with column position 1 (default)
                self.write_to_worksheet(self.make_a1_notation(row_pos), [banner_row], format_specs=banner_format)
                row_pos += 1

        self.row_pos = row_pos

    def add_count_header_section(self, row_pos=None, hpo=None):
        """
        Builds a counts section shaded header row  with the report column headers (Expected, Ready to Sync, etc.)
        """
        if not row_pos:
            row_pos = self.row_pos

        section = self.row_layout.get('count_section')
        # Replace the first (empty) col in the the pre-populated values list with an HPO name, if provided
        if hpo:
            section['values'][0] = hpo
        # A header section covers all cells in the row
        self.write_to_worksheet(self.make_a1_notation(row_pos, end_col=self.sheet_cols),
                                section.get('values'), format_specs=section.get('format'))
        self.row_pos = row_pos + 1

    def add_consent_counts(self, df, row_pos=None, org=None, show_all_counts=False):
        """
          Builds and populates a subsection of rows, with one row per consent type, indicating its status/error counts
          This method can be called on either the full daily data unfiltered dataframe for the daily total summary
          counts, or an Organization-specific dataframe for its error counts

          :param show_all_counts:  Set to True by caller if lines for consents with 0 error counts should be shown
        """
        if not row_pos:
            row_pos = self.row_pos

        # Using integer ConsentType enum values to compare against consent_file.type (df.type) values
        for consent in CONSENTS_LIST:
            # Organization name string (if present) gets written once to column A in the first pass through this loop
            if org and consent == CONSENTS_LIST[0]:
                self.write_to_worksheet(self.make_a1_notation(row_pos),
                                        [org],
                                        format_specs={'textFormat': {'fontSize': 9, 'bold': True},
                                                      'wrapStrategy': 'wrap' })

            expected_count = df.loc[df.type == consent].shape[0]
            # Won't generate spreadsheet rows for consents that had no entries in the daily validation results
            if not expected_count:
                continue

            ready_count = df.loc[(df.type == consent)\
                           & (df.sync_status != int(ConsentSyncStatus.NEEDS_CORRECTING))].shape[0]

            # Filtered dataframe of records for this consent type in NEEDS_CORRECTING status, for further analysis
            consents_with_errors = df.loc[(df.type == consent)\
                            & (df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING))]
            consent_error_count = consents_with_errors.shape[0]

            if not consent_error_count and not show_all_counts:
                # No errors/nothing to report for this consent type
                continue

            # Starting column position of values to be written out at conclusion of this pass
            first_val_column = DAILY_REPORT_COLUMN_MAP.get('consent_type')
            consent_summary_values = [str(ConsentType(consent)), expected_count, ready_count, consent_error_count]
            tracked_error_values = [0]  # The first index will be total error count for all errors for this consent
            if consent_error_count:
                #  Build a list of counts for each tracked error type.  Ending column position for the row write will
                # be the last of the tracked errors columns
                total_errors = 0
                last_val_column = DAILY_REPORT_COLUMN_MAP.get(TRACKED_CONSENT_ERRORS[-1])
                for error in TRACKED_CONSENT_ERRORS:
                    # Pandas: sum all the 1s in this error type df column. Cast result from int64 to int for gsheets
                    error_count = int(consents_with_errors[error].sum())
                    if error_count:
                        tracked_error_values.append(error_count)
                        total_errors += error_count
                    else:
                        # Suppress writing 0s to the spreadsheet individual error columns, for better readability.
                        # Only columns with an error count to report will have values in them.
                        tracked_error_values.append(None)

                # Update the final tally of total count of all errors for this consent
                tracked_error_values[0] = total_errors

            else:
                # No errors exist for this consent, just writing out the summary values
                last_val_column = DAILY_REPORT_COLUMN_MAP.get('total_errors')

            cell_range = self.make_a1_notation(row_pos,
                                               start_col=first_val_column,
                                               end_col=last_val_column)

            # Combine the summary values and the error count values into one list containing all the row's values
            self.write_to_worksheet(cell_range, consent_summary_values + tracked_error_values)
            row_pos += 1

        self.row_pos = row_pos + 1

    def add_daily_errors(self):
        """"
        This method is called when there is a daily error count.   It will generate HPO/Organization-specific
        breakdowns of the consent error metrics.  Only organizations for which there were associated errors will
        be included in the report output.
        """
        self.add_banner_text_row('counts_by_org', row_pos=self.row_pos + 1)

        # Iterate through list of distinct HPOs found in the full daily results dataframe.
        # Includes UNSET HPO (Mapped to '(No HPO Details)' in the daily report SQL query
        hpos = self.consent_df['hpo'].unique()
        for hpo in sorted(hpos):
            hpo_df = self.consent_df[self.consent_df.hpo == hpo]   # Yields an HPO-filtered dataframe

            # If any rec associated with this HPO has consents with errors,  create HPO block section header
            if hpo_df.loc[hpo_df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING)].shape[0]:
                self.add_count_header_section(hpo=hpo)
            else:
                continue

            # Iterate over distinct organizations in the HPO dataframe and build error report for each
            # May include null/UNSET organization (mapped to '(No Organization Details)' by daily report SQL query
            orgs = hpo_df['organization'].unique()
            for org in sorted(orgs):
                org_df = hpo_df[hpo_df.organization == org]   # Yields an Org-filtered dataframe from the HPO frame
                # If this org had consent errors, add the error counts
                if org_df.loc[org_df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING)].shape[0]:
                    self.add_consent_counts(org_df, row_pos=self.row_pos + 1, org=org)
                    # Add a bottom border after the org counts
                    self.write_to_worksheet(self.make_a1_notation(self.row_pos - 1, end_col=self.sheet_cols),
                                            [],  # No cell data, just formatting
                                            format_specs={'borders': {'bottom': {'style': 'SOLID'}}})
                else:
                    continue

    def add_daily_summary(self):
        """ Add content that appears on every daily consent validation report regardless of errors """

        self.add_banner_text_row('report_date')
        # Add any explanatory text / details about the report that have been included in the layout
        self.add_banner_text_row('report-notes', row_pos=self.row_pos+1)
        if not self.consent_errors_found:
            self.add_banner_text_row('no_errors', row_pos=self.row_pos + 1)
        self.add_banner_text_row('total_consent_counts', row_pos=self.row_pos + 1)
        # Daily summary counts for all the consents that were processed.  Show all counts regardless of errors
        self.add_count_header_section(hpo='All Entities')
        self.add_consent_counts(self.consent_df, show_all_counts=True)
        # Add a bottom border after the summary counts
        self.write_to_worksheet(self.make_a1_notation(self.row_pos-1, end_col=self.sheet_cols),
                                [], # No cell data, just formatting
                                format_specs={'borders': {'bottom': { 'style': 'SOLID_THICK'}}} )

    def create_daily_report(self, spreadsheet):
        """
        Add a new daily report tab/sheet to the google sheet file, with the validation details for that date
        """
        if not self.args.csv_only:
            existing_sheets = spreadsheet.worksheets()
            # Perform rolling deletion of the oldest reports so we keep a pre-defined maximum number of daily reports
            # NOTE:  this assumes all the reports in the file were generated in order, with the most recent date at the
            # leftmost tab (index 0).   This deletes sheets from the existing_sheets list, starting at the rightmost tab
            for ws_index in range(len(existing_sheets), self.max_daily_reports-1, -1):
                spreadsheet.del_worksheet(existing_sheets[ws_index-1])

            # Add the new worksheet (to leftmost tab position / index 0)
            self.worksheet = spreadsheet.add_worksheet(self.report_date.strftime("%b %d %Y"),
                                                       rows=self.sheet_rows,
                                                       cols=self.sheet_cols,
                                                       index=1)
            self.add_daily_summary()

        if self.consent_errors_found:
            if not self.args.csv_only:
                # Google sheets doesn't have flexible/multiple freezing options.  Freeze all rows above the current
                # position.  Makes so HPO/Org-specific section(s) scrollable while still seeing column header names
                self.worksheet.freeze(rows=self.row_pos - 1)
                self.add_daily_errors()
            if not self.args.sheet_only:
                self.create_csv_errors_file()
        else:
            _logger.info('No errors to report')

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

        # To Do:  refactor so common consent from the daily report can be leveraged into the weekly "all unresolved"
        # report
        self.create_daily_report(gs_file)



def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.  NOTE:  This tool defaults to PRODUCTION project/service account
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default=RdrEnvironment.PROD.value)  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account",
                        default=f'configurator@{RdrEnvironment.PROD.value}.iam.gserviceaccount.com') #noqa
    # TODO:  Replace CONSENT_DOC_ID environment variable with reading the doc ID value from the config settings
    parser.add_argument("--doc-id", type=str,
                        help="A google sheet ID which can override a CONSENT_DOC_ID env var")
    parser.add_argument("--report-type", type=str, default="daily_uploads", metavar='REPORT',
                        help="Report to generate.  Default is daily_uploads")
    parser.add_argument("--report-date", type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help="Date of the consents (authored) in YYYY-MM-DD format.  Default is yesterday's date")
    parser.add_argument("--csv-file", type=str,
                        help="output filename for the CSV error list. " +\
                                           " Default is YYYYMMDD_consent_errors.csv where YYYYMMDD is the report date")
    parser.add_argument("--sheet-only", default=False, action="store_true",
                        help="Only generate the googlesheet report, skip generating the CSV file")
    parser.add_argument("--csv-only", default=False, action="store_true",
                        help="Only generate the CSV errors file, skip generating google sheet content")
    parser.epilog = f'Possible REPORT types: {{{",".join(REPORT_TYPES.keys())}}}.'
    args = parser.parse_args()


    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ProgramTemplateClass(args, gcp_env)
        exit_code = process.execute()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
