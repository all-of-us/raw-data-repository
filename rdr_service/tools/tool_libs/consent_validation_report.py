#! /bin/env python
#
# Temporary tool for manually generating consent validation metrics (until it can be automated by dashboard team)
# Also creates CSV files for PTSC with information about consent errors.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys
import os
import csv
import gspread
import gspread_formatting as gsfmt
import pandas
from datetime import date, datetime, timedelta
from gspread.utils import rowcol_to_a1


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
# data from the RDR consent_file table.
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

# Tuples with column header text and column number (1-based) for generating consent error count sections
CONSENT_ERROR_COUNT_COLUMNS = [
    ('Consent Type', 3),
    ('Expected', 4),
    ('Ready to Sync', 5),
    ('Participants With Unresolved Issues', 6),
    ('Consent Files With Errors', 7),
    ('Total Errors', 8),
    ('Missing File', 9),
    ('Signature Missing', 10),
    ('Signature Date Invalid', 11),
    ('Invalid DOB', 12),
    ('Age at Primary Consent < 18', 13),
    ('Checkbox Unchecked', 14),
    ('Non-VA Consent for VA Particip[ant', 15),
    ('VA Consent for Non-VA Participant', 16)

]

# Maps the currently validated consent types to the related status/authored fields to query from participant_summary
CONSENT_PARTICIPANT_SUMMARY_FIELDS = {
    # For PRIMARY:  use earliest consent authored (to distinguish from PrimaryConsentUpdate authored, which are not
    # yet included in the validation)
    ConsentType.PRIMARY : ('consent_for_study_enrollment', 'consent_for_study_enrollment_first_yes_authored'),
    ConsentType.CABOR: ('consent_for_cabor', 'consent_for_cabor_authored'),
    ConsentType.EHR: ('consent_for_electronic_health_records',
                      'consent_for_electronic_health_records_first_yes_authored'),
    ConsentType.GROR: ('consent_for_genomics_ror', 'consent_for_genomics_ror_authored')
    # TODO:  Enable once the retrospective validations of the Cohort 1 consent update are vetted for false positives
    # ConsentType.PRIMARY_UPDATE: ('consent_for_study_enrollment', 'consent_for_study_enrollment_authored')
}

# List of currently validated consent type values as ints, for pandas filtering of consent_file.type values
CONSENTS_LIST = [int(v) for v in CONSENT_PARTICIPANT_SUMMARY_FIELDS.keys()]

# Raw SQL used initially for fast prototyping of reports.  These reports will be taken over by dashboard team
CONSENT_REPORT_SQL_BODY =  """
            SELECT cf.participant_id,
                   ps.date_of_birth,
                   CASE
                      WHEN (h.name IS NOT NULL and h.name != 'UNSET') THEN h.name
                      ELSE '(Unpaired)'
                   END AS hpo,
                   CASE
                      WHEN o.display_name IS NOT NULL THEN o.display_name ELSE '(No organization pairing)'
                   END AS organization,
                   DATE(ps.{authored_field}) AS consent_authored_date,
                   cf.sync_status,
                   cf.type,
                   cf.file_path,
                   cf.file_upload_time,
                   -- Adding signing date details to data pull to support new filtering logic on the results
                   cf.signing_date,
                   cf.expected_sign_date,
                   -- Calculated fields to generate 0 or 1 values for the known tracked error conditions
                   -- (1 if error found)
                   NOT cf.file_exists AS missing_file,
                   (cf.file_exists and NOT is_signature_valid) AS signature_missing,
                   (cf.is_signature_valid and NOT cf.is_signing_date_valid) AS invalid_signing_date,
                   -- Invalid DOB conditions: DOB missing, DOB before defined cutoff, DOB in the future, or
                   -- DOB later than the consent authored date
                   (ps.date_of_birth is null or ps.date_of_birth > "{report_date}"
                    or (ps.consent_for_study_enrollment_first_yes_authored is not null
                        and TIMESTAMPDIFF(YEAR, ps.consent_for_study_enrollment_first_yes_authored,
                                          ps.date_of_birth) > 124))
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
#   and where the consent status in participant_summary is SUBMITTED (1) -- the validation process is only interested
#   in newly authored "yes"/SUBMITTED consents
# - Find corresponding consent_file entries for the consent type, in NEEDS_CORRECTING/READY_TO_SYNC/SYNC_COMPLETE
DAILY_CONSENTS_SQL_FILTER = """
            WHERE cf.type = {consent_type}
                  AND ps.{status_field} = 1
                  AND DATE(ps.{authored_field}) = "{report_date}"
                  AND cf.sync_status IN (1,2,4)
    """

# -- Weekly report queries --

# Filter to produce a report of all remaining NEEDS_CORRECTING consents of a specified type, up to and including the
# specified end date for this report
ALL_UNRESOLVED_ERRORS_SQL_FILTER = """
            WHERE cf.type = {consent_type}
                  AND DATE(ps.{authored_field}) <= "{end_date}"
                  AND cf.sync_status = 1
    """

# Filter for generating stats on file issues resolved by retransmission (OBSOLETE consent_file entries)
# The last modified timestamp for an OBSOLETE record should reflect when it was moved into OBSOLETE status; include
# resolutions up to the specified end date for this report
ALL_RESOLVED_SQL = """
           SELECT cf.participant_id,
                  cf.type,
                  DATE(cf.modified) AS resolved_date
           FROM consent_file cf
           WHERE cf.sync_status = 3 AND DATE(cf.modified) <= "{end_date}"
"""

# TODO:  Remove this when we expand consent validation to include CE consents
VIBRENT_SQL_FILTER = ' AND ps.participant_origin = "vibrent" '

# Weekly report SQL for validation burndown counts
CONSENTED_PARTICIPANTS_COUNT_SQL = """
    SELECT COUNT(DISTINCT ps.participant_id)  consented_participants
    FROM participant_summary ps
    JOIN participant p ON p.participant_id = ps.participant_id
         AND p.is_test_participant = 0 and (p.is_ghost_id is null or not p.is_ghost_id) and p.hpo_id != 21
    WHERE DATE(ps.consent_for_study_enrollment_first_yes_authored) <= "{end_date}"
"""

VALIDATED_PARTICIPANTS_COUNT_SQL = """
    SELECT COUNT(DISTINCT cf.participant_id) validated_participants
    FROM consent_file cf
    JOIN  participant ps ON ps.participant_id = cf.participant_id
         AND ps.is_test_participant = 0 and (ps.is_ghost_id is null or not ps.is_ghost_id) and ps.hpo_id != 21
    WHERE DATE(cf.created) <= "{end_date}"
"""

# Define the allowable --report-type arguments
REPORT_TYPES = ['daily_uploads', 'weekly_status']


class SafeDict(dict):
    """
    See: https://stackoverflow.com/questions/17215400/format-string-unused-named-arguments
    Used with str.format_map() to allow partial formatting/replacement of placeholder values in a string
    without incurring KeyError. E.g.: '{lastname}, {firstname} {lastname}'.format_map(SafeDict(lastname='Bond'))
    yields the partially formatted result: 'Bond, {firstname} Bond'

     This helper class will be used when formatting SQL template strings that need to have some of their placeholders
     filled in before the values for the others can be determined.
    """
    def __missing__(self, key):
        return '{' + key + '}'


class ConsentReport(object):
    """"
        The ConsentReport class will contain attributes common to both the daily consent validation report and the
        weekly consent validation status report.   Methods common to both consent reports reside in this parent class
    """
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.db_conn = None
        if not args.report_type in REPORT_TYPES:
            raise ValueError(f'invalid report type option: {args.report_type}')
        else:
            self.report_type = args.report_type

        # Defaults, overridden as needed by child classes
        self.worksheet = None
        self.sheet_rows = 500
        # The column indexes/numbers are the second element in the CONSENT_ERROR_COUNT_COLUMN tuples. Get the column
        # count for the sheet by finding the max column number
        self.sheet_cols = max([column_tuple[1] for column_tuple in CONSENT_ERROR_COUNT_COLUMNS])

        # A pandas dataframe to be populated with results of the specific report (daily or weekly) SQL query
        self.consent_df = None

        # Position tracker updated as content is added to the daily report worksheet
        self.row_pos = 1

        # Lists appended to as the report content is generated, containing the cell data and the cell formatting
        # The resulting data will be written out in a gspread batch_update() call, and the formatting will be applied
        # via a gspread-formatting format_cell_ranges() batch call
        self.report_data = []
        self.report_formatting = []

        # Commonly used cell formats for the consent reports, applied via gspread-formatting module (imported as gsfmt)
        # See https://libraries.io/pypi/gspread-formatting for information on its implementation of
        # googlesheets v4 API CellFormat classes and how to nest its classes
        self.format_specs = {
            'bold_text': gsfmt.cellFormat(textFormat=gsfmt.textFormat(bold=True, fontSize=12)),
            'bold_small_wrapped': gsfmt.cellFormat(textFormat=gsfmt.textFormat(bold=True, fontSize=9),
                                                   wrapStrategy='WRAP'),
            'italic_text': gsfmt.cellFormat(textFormat=gsfmt.textFormat(italic=True, fontSize=12)),
            'legend_text': gsfmt.cellFormat(textFormat=gsfmt.textFormat(fontSize=10,italic=True,
                                                                        foregroundColor=gsfmt.color(0, 0, 1))),
            'column_header': gsfmt.cellFormat(textFormat=gsfmt.textFormat(bold=True),
                                              wrapStrategy='WRAP',
                                              verticalAlignment='MIDDLE'),
            'count_section_header_row': gsfmt.cellFormat(textFormat=gsfmt.textFormat(bold=True),
                                                         wrapStrategy='WRAP',
                                                         backgroundColor=gsfmt.color(0.02, 0.8, 0.4),
                                                         verticalAlignment='MIDDLE',
                                                         borders=gsfmt.borders(top=gsfmt.border('SOLID_MEDIUM'),
                                                                               bottom=gsfmt.border('SOLID_MEDIUM'))),
            'solid_border': gsfmt.cellFormat(borders=gsfmt.borders(top=gsfmt.border('SOLID'))),
            'solid_thick_border': gsfmt.cellFormat(borders=gsfmt.borders(bottom=gsfmt.border('SOLID_THICK')))
        }

    def _add_format_spec(self, fmt_name_key: str, fmt_spec : gsfmt.CellFormat):
        """ Add a new format spec to the instance format_specs list """
        if not len(fmt_name_key) or not isinstance(fmt_spec, gsfmt.CellFormat):
            raise (ValueError, "Invalid format specification data")
        else:
            self.format_specs[fmt_name_key] = fmt_spec

    def _connect_to_rdr_replica(self):
        """ Establish a connection to the replica RDR database for reading consent validation data """
        self.gcp_env.activate_sql_proxy(replica=True)
        self.db_conn = self.gcp_env.make_mysqldb_connection()

    def _has_needs_correcting(self, dframe):
        """ Check if the dataframe provided has any records in a NEEDS_CORRECTING state """
        return (dframe.loc[dframe.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING)].shape[0] > 0)

    def _make_a1_notation(self, start_row, start_col=1, end_row=None, end_col=None):
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

    def _add_report_rows(self, cell_range, value_list=[]):
        """
        Adds to the list of report_data elements that will be passed to gspread batch_update().
        Example of a data element dict:
            { 'range': 'A1:N5',
               'values': [[<cell values for row 1 A1:N1 columns], ..., [<cell values for row 5 A5:N5 columns]]
            }

        """
        if not cell_range or not len(value_list):
            raise(ValueError, "Invalid data object for spreadsheet")

        self.report_data.append({
            'range': cell_range,
            'values': value_list
        })

    def _add_report_formatting(self, cell_range: str, fmt_spec: gsfmt.CellFormat):
        """
        Adds an element to a list of formatting spec elements that will be passed to gspread-formatting
        format_cell_ranges()

        See: https://libraries.io/pypi/gspread-formatting

        """
        self.report_formatting.append((cell_range, fmt_spec))

    def _add_text_rows(self, text_rows=[], format_spec=None, row_pos=None):
        """
          Add a row or rows with the requested text (e.g., Report Date line, Notes, etc.) to the report content
        """
        if not row_pos:
            row_pos = self.row_pos

        end_of_text_pos = row_pos + len(text_rows)
        cell_range = self._make_a1_notation(row_pos, end_row=end_of_text_pos)
        self._add_report_rows(cell_range, text_rows)
        if format_spec:
            self._add_report_formatting(cell_range, format_spec)

        self.row_pos = end_of_text_pos

    def _add_consent_issue_count_header_section(self, row_pos=None, hpo=''):
        """
        Builds a counts section shaded header row with the all the column headers.  This section header is used
        for both the aggregate (all entities) counts, as well as for each section when the error counts are broken down
        by HPO/Org
        """
        if not row_pos:
            row_pos = self.row_pos

        # The column header string is the first element of each tuple in the CONSENT_ERROR_COUNT_COLUMNS tuple list
        count_headers = [column_tuple[0] for column_tuple in CONSENT_ERROR_COUNT_COLUMNS]

        # Kludge:  minor customization of otherwise shared data between daily and weekly reports.
        if self.report_type == 'weekly_status':
            # Drop the Expected and Ready to Sync columns; aren't helpful to show in weekly report, which is tracking
            # consents with errors only
            count_headers = [h for h in count_headers if h not in ['Expected', 'Ready to Sync']]

        # Column A has HPO name, Column B intentionally blank, then add the rest of the error count columns
        hpo_header_row = [hpo, ''] + count_headers

        cell_range = self._make_a1_notation(row_pos, end_col=self.sheet_cols)
        # Add this single header row and its formatting to the report content
        self._add_report_rows(cell_range, [hpo_header_row])
        self._add_report_formatting(cell_range, self.format_specs.get('count_section_header_row'))
        self.row_pos = row_pos + 1

    def _add_consent_issue_counts(self, df, row_pos=None, org=None, show_all_counts=False):
        """
          Builds and populates a subsection of rows, with one row per consent type, indicating its status/error counts
          :param df:  The dataframe to operate on. This could be data for all entities to generate overall counts, or
                      it could be a dataframe filtered by organization for the organization-specific counts
          :param show_all_counts:  Set to True by caller if lines for consents with 0 error counts should be shown
        """
        if not row_pos:
            row_pos = self.row_pos

        # Track if we've already generated a row containing the organization name.  It's only included with the first
        # line / first consent that has associated errors
        org_string_written = False
        for consent in CONSENTS_LIST:
            expected_count = df.loc[df.type == consent].shape[0]
            # Won't generate report rows for consents that had no entries in the daily validation results
            if not expected_count:
                continue

            ready_count = df.loc[(df.type == consent)\
                           & (df.sync_status != int(ConsentSyncStatus.NEEDS_CORRECTING))].shape[0]

            # Create a filtered dataframe of records for this consent in NEEDS_CORRECTING status, for further analysis
            consents_with_errors = df.loc[(df.type == consent)\
                            & (df.sync_status == int(ConsentSyncStatus.NEEDS_CORRECTING))].reset_index()

            consent_error_count = consents_with_errors.shape[0]
            # Count of distinct (pandas nunique() = number of unique) participant_id values having NEEDS_CORRECTING:
            participant_count = consents_with_errors['participant_id'].nunique()

            if not consent_error_count and not show_all_counts:
                # No errors/nothing to report for this consent type
                continue

            # The organization name (bolded) only appears in Column A for the first row generated.
            # Column B is intentionally blank
            if org and not org_string_written:
                row_values = [org, '']
                self._add_report_formatting(self._make_a1_notation(row_pos),
                                            self.format_specs.get('bold_small_wrapped'))
                org_string_written = True
            else:
                row_values = ['', '']

            # Kludge:  Some minor customization of otherwise mostly shared data between daily and weekly reports
            if self.report_type == 'weekly_status':
                # Weekly outstanding issues report does not have Expected / Ready to Sync columns
                row_values.extend([str(ConsentType(consent)), int(participant_count), consent_error_count])
            else:
                row_values.extend([ str(ConsentType(consent)), expected_count, ready_count,
                                    int(participant_count), consent_error_count])

            tracked_error_values = []
            total_errors = 0
            if consent_error_count:
                for error in TRACKED_CONSENT_ERRORS:
                    if error in ['invalid_dob', 'invalid_age_at_consent'] and consent != int(ConsentType.PRIMARY):
                        # DOB issues only apply for PRIMARY consent
                        error_count = 0
                    else:
                        # Pandas: sum all the values in error type column (will be 0 or 1). Cast result from float
                        error_count = int(consents_with_errors[error].sum())

                    if error_count:
                        tracked_error_values.append(error_count)
                        total_errors += error_count
                    else:
                        # Suppress writing 0s to the spreadsheet individual error columns, for better readability.
                        # Only columns with an error count to report will have values in them.
                        tracked_error_values.append(None)

            row_values.append(total_errors)
            row_values.extend(tracked_error_values)
            self._add_report_rows(self._make_a1_notation(row_pos, end_col=len(row_values)), [row_values])
            row_pos += 1

        self.row_pos = row_pos

    def _add_errors_by_org(self, df=None):
        """"
        Generate HPO/Organization-specific breakdowns of the consent error metrics.
        Only organizations for which there were associated errors will be included in the report output.

        """
        if df is None:
            df = self.consent_df

        # Iterate over list of distinct (pandas: unique() ) HPO names in the dataframe
        hpos = df['hpo'].unique()
        for hpo in sorted(hpos):
            hpo_df = df[df.hpo == hpo]   # Yields an HPO-filtered dataframe
            if self._has_needs_correcting(hpo_df):
                self._add_consent_issue_count_header_section(hpo=hpo, row_pos=self.row_pos + 1)
                # Iterate over distinct organizations in the HPO dataframe and build error report for any org having
                # records in NEEDS_CORRECTING status
                orgs = hpo_df['organization'].unique()
                for org in sorted(orgs):
                    org_df = hpo_df[hpo_df.organization == org]   # Yields an Org-filtered dataframe from the HPO frame
                    if self._has_needs_correcting(org_df):
                        # Visual border to separate from the previous organization subsection
                        self._add_report_formatting(self._make_a1_notation(self.row_pos, end_col=self.sheet_cols),
                                                    self.format_specs.get('solid_border'))

                        self._add_consent_issue_counts(org_df, org=org, row_pos=self.row_pos)

        # Draw final border for the entire HPO section after all the organization subsections are generated
        self._add_report_formatting(self._make_a1_notation(self.row_pos - 1, end_col=self.sheet_cols),
                                    self.format_specs.get('solid_thick_border'))

    def _remove_potential_false_positives_for_consent_version(self, df):
        """
        Found some cases where the validation on the consent files may have run before the participant pairing was
        completed.  This was resulting in some potential false positives for va_consent_for_non_va errors.  For now,
        ignore any NEEDS_CORRECTING records where participant is currently paired to VA HPO, and the only error flagged
        was va_consent_for_non_va
        """

        # Pandas:  find all the records we want to keep and make a new dataframe out of the result.  Inverts the
        # "and" conditions above for the known false positives in order to find everything but those records
        filtered_df = df.loc[(df.sync_status != int(ConsentSyncStatus.NEEDS_CORRECTING)) |\
                              (df.hpo != 'VA') | (df.va_consent_for_non_va == 0) |\
                              (df.missing_file == 1) | (df.invalid_dob == 1) | (df.invalid_age_at_consent == 1) |\
                              (df.checkbox_unchecked == 1) | (df.non_va_consent_for_va == 1)].reset_index()

        return filtered_df

    def _get_consent_validation_dataframe(self, sql_template):
        """
        Queries the RDR participant summary/consent_file tables for entries of each consent type for which validation
        has been implemented, and merges the results into a single pandas dataframe

        :param sql_template  A SQL string with {authored_field}, {status_field}, and {consent_type} placeholders
                             that will be filled in as data for each consent type is queried
        """
        if not self.db_conn:
            raise (EnvironmentError, 'No active DB connection object')

        df = pandas.DataFrame()
        for consent_int in CONSENTS_LIST:
            sql = sql_template
            # The tuple retrieved from the CONSENT_PARTICIPANT_SUMMARY_FIELDS dict has two elements like:
            # ('consent_for_study_enrollment', 'consent_for_study_enrollment_first_yes_authored')
            consent_status_field = CONSENT_PARTICIPANT_SUMMARY_FIELDS[ConsentType(consent_int)][0]
            consent_authored_field = CONSENT_PARTICIPANT_SUMMARY_FIELDS[ConsentType(consent_int)][1]
            sql = sql.format_map(SafeDict(consent_type=consent_int,
                                          status_field=consent_status_field,
                                          authored_field=consent_authored_field))
            consent_df = pandas.read_sql_query(sql, self.db_conn)
            # Replace any null values in the calculated error flag columns  with (uint8 vs. pandas default float) zeroes
            for error_type in TRACKED_CONSENT_ERRORS:
                consent_df = consent_df.fillna({error_type: 0}).astype({error_type: 'uint8'})

            df = df.append(consent_df)

        # Temporary?  Attempt to filter false positives for va_consent_for_non_va consent version errors out of the
        # generated dataframe
        df = self._remove_potential_false_positives_for_consent_version(df)

        return df

    def _write_report_content(self):
        """ Make the batch calls to add all the cell data and apply the formatting to the spreadsheet """
        self.worksheet.batch_update(self.report_data)
        gsfmt.format_cell_ranges(self.worksheet, self.report_formatting)

class DailyConsentReport(ConsentReport):
    """
    Class to implement the generation of the daily consent validation report for newly authored consents, and a
    CSV file with error details if errors were detected
    """
    def __init__(self, args, gcp_env: GCPEnvConfigObject):

        super().__init__(args, gcp_env)

        if args.doc_id:
            self.doc_id = args.doc_id
        else:
            self.doc_id = os.environ['DAILY_CONSENT_DOC_ID']
        if not self.doc_id:
            raise ValueError('Please use the --doc-id arg or export DAILY_CONSENT_DOC_ID environment var')
        if args.report_date:
            self.report_date = args.report_date
        else:
            # Default to yesterday's date as the filter for consent authored date
            self.report_date = datetime.now() - timedelta(1)
        if args.csv_file:
            self.csv_filename = args.csv_file
        else:
            self.csv_filename = f'{self.report_date.strftime("%Y%m%d")}_consent_errors.csv'

        self.report_sql = CONSENT_REPORT_SQL_BODY + DAILY_CONSENTS_SQL_FILTER + VIBRENT_SQL_FILTER

        # Max columns for the daily sheet (max column index value from the CONSENT_ERROR_COUNT_COLUMNS tuples)
        self.sheet_cols = max([column[1] for column in CONSENT_ERROR_COUNT_COLUMNS])
        # Number of days/worksheets to archive in the file (will do rolling deletion of oldest daily worksheets/tabs)
        self.max_daily_reports = 32 # A month's worth + an extra sheet to contain a legend / notes as needed
        self.consent_errors_found = False

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

    def add_daily_summary(self):
        """ Add content that appears on every daily consent validation report regardless of errors """

        report_title = 'Report for consents authored on: {} 12:00AM-11:59PM UTC (generated on {} Central)'.\
            format(self.report_date.strftime("%b %-d, %Y"), datetime.now().strftime(("%x %X")))

        report_notes = [
            ['Notes:'],
            ['Validation is currently only done for PTSC consent files (does not include CareEvolution)'],
            ['Checkbox validation currently only performed on GROR consents'],
            ['Total Errors can exceed Consents with Errors if any consents had multiple validation errors']
        ]

        self._add_text_rows(text_rows=[[report_title]], format_spec=self.format_specs.get('bold_text'))
        # Add any explanatory text / details about the report that have been included in the layout
        self._add_text_rows(text_rows=report_notes, format_spec=self.format_specs.get('legend_text'),
                            row_pos=self.row_pos + 1)

        if not self._has_needs_correcting(self.consent_df):
            self._add_text_rows(text_rows=[['No consent validation errors detected']],
                                format_spec=self.format_specs.get('italic_text'), row_pos=self.row_pos+1)

        # Daily summary counts for all the recently authored consents that were processed (regardless of errors)
        self._add_text_rows([['Total Consent Validation Counts']],
                            format_spec=self.format_specs.get('bold_text'), row_pos=self.row_pos+1)
        self._add_consent_issue_count_header_section(hpo='All Entities')
        self._add_consent_issue_counts(self.consent_df, show_all_counts=True)

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

        if self._has_needs_correcting(self.consent_df):
            if not self.args.csv_only:
                # Google sheets doesn't have flexible/multiple freezing options.  Freeze all rows above the current
                # position.  Makes so HPO/Org-specific section(s) scrollable while still seeing column header names
                self.worksheet.freeze(rows=self.row_pos - 1)
                self._add_text_rows(text_rows=[['Consent errors by HPO/Organization']],
                                    format_spec=self.format_specs.get('bold_text'))
                self._add_errors_by_org()
            if not self.args.sheet_only:
                self.create_csv_errors_file()
        else:
            _logger.info('No errors to report')

        self._write_report_content()

        _logger.info('Report complete')


    def execute(self):
        """
        Execute the DailyConsentReport builder
        """

        # Set up DB and googlesheets doc access
        self._connect_to_rdr_replica()
        service_key_info = gcp_get_iam_service_key_info(self.gcp_env.service_key_id)
        gs_creds = gspread.service_account(service_key_info['key_path'])
        gs_file = gs_creds.open_by_key(self.doc_id)

        # Retrieve the daily data and build the report.  Partial string substitution for the SQL statments is done
        # here; the remaining substitutions occur in the _get_consent_validation_dataframe() method
        self.consent_df = self._get_consent_validation_dataframe(
            self.report_sql.format_map(SafeDict(report_date=self.report_date.strftime("%Y-%m-%d")))
        )
        self.create_daily_report(gs_file)


class WeeklyConsentReport(ConsentReport):
    """
    Class to implement the weekly consent validation status report, which includes details of all retrospective
    validation errors that are still pending resolution
    """
    def __init__(self, args, gcp_env: GCPEnvConfigObject):

        super().__init__(args, gcp_env)

        if args.doc_id:
            self.doc_id = args.doc_id
        else:
            self.doc_id = os.environ['WEEKLY_CONSENT_DOC_ID']
        if not self.doc_id:
            raise ValueError('Please use the --doc-id arg or export WEEKLY_CONSENT_DOC_ID environment var')

        # Default to yesterday's date as the end of the weekly report range, and a week prior to that as start date
        self.end_date = args.end_date or (datetime.now() - timedelta(1))
        self.start_date = args.start_date or (self.end_date - timedelta(7))
        self.report_date = datetime.now()
        self.report_sql = CONSENT_REPORT_SQL_BODY + ALL_UNRESOLVED_ERRORS_SQL_FILTER + VIBRENT_SQL_FILTER
        self.sheet_rows = 800
        # Number of days/worksheets to archive in the file (will do rolling deletion of oldest daily worksheets/tabs)
        self.max_weekly_reports = 9 # Two month's worth + an extra sheet to contain a legend / notes as needed
        self.consent_errors_found = False
        # Additional dataframe (after self.consent_df) that will hold results from querying resolved/OBSOLETE issues
        self.resolved_df = None
        # Format specs only used in weekly report
        self._add_format_spec('burndown_header_row',
                              gsfmt.cellFormat(backgroundColor=gsfmt.color(0.87, 0.46, 0),
                                               textFormat=gsfmt.textFormat(bold=True,fontSize=10))
        )
        self._add_format_spec('burndown_column_headers',
                              gsfmt.cellFormat(textFormat=gsfmt.textFormat(bold=True, fontSize=9),
                                               backgroundColor=gsfmt.color(1, .84, 0),
                                               wrapStrategy='WRAP',
                                               verticalAlignment='MIDDLE')
        )

    def remove_potential_false_positives_for_missing_signature(self, df):
        """
        A temporary method to ignore NEEDS_CORRECTING consents if they fit a profile observed during retrospective
        validation, where we know the PDF validation tool is failing to find valid signing date/signature details.
        NEEDS_CORRECTING records should be ignored for now if:
        - missing_file field is 0 (file exists) AND
        - expected_sign_date < 2018-07-13 AND
        - signing_date is null AND
        - Has no other tracked error fields set to 1/True (except either signature_missing or invalid_signing_date)

        Returns a dataframe with all the records except those that match the above criteria
        """

        filter_date = date(year=2018,month=7,day=13)
        # Pandas:  find all the records we want to keep and make a new dataframe out of the result.  Inverts the
        # "and" conditions above for the known false positives in order to find everything but those records
        filtered_df = df.loc[(df.missing_file == 1) | (df.invalid_dob == 1) | (df.invalid_age_at_consent == 1) |\
                             (df.checkbox_unchecked == 1) | (df.non_va_consent_for_va == 1) |\
                             (df.expected_sign_date >= filter_date) | (df.signing_date.isnull() == False)].reset_index()

        return filtered_df

    def get_resolved_consent_issues_dataframe(self):
        """
        Returns a dataframe of all issues marked OBSOLETE up to and including on the report end date.  OBSOLETE implies
        the file which did not pass validation has been superceded by a new/retransmitted consent file which was
        successfully validated.  In some cases, a consent_file entry may be marked OBSOLETE after a manual inspection/
        issue resolution.
        """
        sql = ALL_RESOLVED_SQL.format_map(SafeDict(end_date=self.end_date.strftime("%Y-%m-%d")))
        resolved_df = pandas.read_sql_query(sql, self.db_conn)

        return resolved_df

    def add_weekly_validation_burndown_section(self):
        """
        Creates a summary section tracking progress of retrospective consent validations
        Displays counts of the individual participants whose consents were validated with with no issues detected,
        participants with unresolved consent file issues, and participants whose consents are yet to be validated
        """
        cursor = self.db_conn.cursor()

        # Gets a count of non-test/ghost participants with a participant_summary (e.g, RDR got a primary consent),
        # if the primary consent authored date was on/before the end date for this report
        sql = CONSENTED_PARTICIPANTS_COUNT_SQL + VIBRENT_SQL_FILTER
        cursor.execute(sql.format_map(SafeDict(end_date=self.end_date.strftime("%Y-%m-%d"))))
        consented_count = cursor.fetchone()[0]

        # Gets a count of non-test/ghost participants whose consents have been validated
        # (participant has entries in consent_file table), if the consent_file entry was created on/before the
        # end date for this report
        sql = VALIDATED_PARTICIPANTS_COUNT_SQL + VIBRENT_SQL_FILTER
        cursor.execute(sql.format_map(SafeDict(end_date=self.end_date.strftime("%Y-%m-%d"))))
        validated_count = cursor.fetchone()[0]

        # Pandas: Gets the number of unique participant_id values from the main (unresolved errors) dataframe
        # that was created at the start of the weekly report generation
        participants_with_errors = self.consent_df['participant_id'].nunique()

        participants_no_issues = validated_count - participants_with_errors
        participants_need_validation = consented_count - validated_count

        burndown_data = [
            ['DRC CONSENT VALIDATION BURNDOWN'],
            ['',
             'Total Consented Participants',
             'Participants With No Consent Issues Detected',
             'Participants With Unresolved Issues (for 1 or more consent types)',
             'Participants Not Yet Validated'],
            ['Participant Counts',
             consented_count,
             participants_no_issues,
             participants_with_errors,
             participants_need_validation]
        ]

        start_burndown_row = self.row_pos
        end_burndown_row= start_burndown_row + len(burndown_data)
        burndown_cell_range = self._make_a1_notation(start_burndown_row, end_col=5, end_row=end_burndown_row)
        self._add_report_rows(burndown_cell_range, burndown_data)

        # Format the burndown sub-table header and column headers
        self._add_report_formatting(self._make_a1_notation(start_burndown_row, end_col=5),
                                    self.format_specs.get('burndown_header_row'))
        self._add_report_formatting(self._make_a1_notation(start_burndown_row + 1, end_col=5),
                                    self.format_specs.get('burndown_column_headers'))
        # Format the burndown sub-table content row (first column is bolded)
        self._add_report_formatting(self._make_a1_notation(end_burndown_row - 1),
                                    self.format_specs.get('bold_small_wrapped'))

        # Inject whitespace after the validation burndown details
        self.row_pos = end_burndown_row + 3

    def add_weekly_file_issue_burndown_section(self):
        """
        Add a section/sub-table that tracks how many consent issues have been resolved, overall and during the
        date range covered by the report.   "Resolved" means consent_file entries that have been marked
        OBSOLETE as their sync_status, indicating a newer file was received that has passed validation.   The modified
        date of an OBSOLETE entry should also indicate when the resolution occurred.  Do not expect consent_file
        records to be modified any more after being marked OBSOLETE.
        """

        # Count of all resolved (OBSOLETE) consent files, and all of the oustanding issues (main report data in the
        # self.consent_df dataframe).  These dataframes were populated at the start of the report execution
        total_resolved = self.resolved_df.shape[0]
        still_unresolved = self.consent_df.shape[0]

        # Count of OBSOLETE consent files last modified in the report date range.  DATE(modified)  = resolved_date
        resolved_in_report_date_range = self.resolved_df.loc[(self.resolved_df.resolved_date >= self.start_date.date())\
                                                 & (self.resolved_df.resolved_date <= self.end_date.date())].shape[0]
        report_range_start = self.start_date.strftime("%Y-%m-%d")
        report_range_end = self.end_date.strftime("%Y-%m-%d")

        # Add stats on how many consent file issues have been resolved, all time and during report date range
        resolution_counts_data = [
            ['CONSENT FILE ISSUE RESOLUTION BURNDOWN'],
            ['', 'Cumulative file resolutions',
             f'Resolved from {report_range_start} to {report_range_end}',
             'Files pending resolution'
            ],
            ['File counts', total_resolved, resolved_in_report_date_range, still_unresolved]
        ]
        end_resolution_counts_row = self.row_pos + len(resolution_counts_data)
        # Extend the resolution header row by an extra column to align with validation burndown sub-section/table
        resolution_header_row = self._make_a1_notation(self.row_pos, end_col=5)
        resolution_counts_header_row = self._make_a1_notation(self.row_pos+1, end_col=5)
        resolution_counts_data_row = self._make_a1_notation(self.row_pos+2)

        self._add_report_rows(self._make_a1_notation(self.row_pos, end_col=5, end_row=end_resolution_counts_row),
                              resolution_counts_data)
        self._add_report_formatting(resolution_header_row, self.format_specs.get('burndown_header_row'))
        self._add_report_formatting(resolution_counts_header_row,
                                    self.format_specs.get('burndown_column_headers'))
        # Format the burndown sub-table content row (first column is bolded)
        self._add_report_formatting(resolution_counts_data_row,
                                    self.format_specs.get('bold_small_wrapped'))
        self.row_pos = end_resolution_counts_row + 2

    def add_weekly_aggregate_outstanding_counts_section(self):
        """
        Generates a summary of all outstanding issues, by consent type / participants impacted
        """
        outstanding_counts_text_cell = self._make_a1_notation(self.row_pos)
        self._add_report_rows(outstanding_counts_text_cell, [
                    ['Summary of all outstanding consent issues, by consent type / participants impacted']
        ])
        self._add_report_formatting(outstanding_counts_text_cell, self.format_specs.get('bold_text'))
        # Generate the "All outstanding consent issues" summary counts
        self._add_consent_issue_count_header_section(hpo='All Entities', row_pos=self.row_pos + 1)
        self._add_consent_issue_counts(self.consent_df, show_all_counts=True)
        self._add_report_formatting(self._make_a1_notation(self.row_pos - 1, end_col=self.sheet_cols),
                                    self.format_specs.get('solid_thick_border'))
        self.row_pos += 1

    def add_weekly_recent_errors_section(self):
        """
        Provide a breakdown of unresolved issues detected in the report date range, from recently authored consents
        """
        start_date = self.start_date.date()
        end_date = self.end_date.date()
        # Created a filtered dataframe from the main unresolved errors dataframe, where the authored dates for the
        # consents with unresolved issues is within the report date range
        weekly_errors = self.consent_df.loc[(self.consent_df.consent_authored_date >= start_date) &\
                                            (self.consent_df.consent_authored_date <= end_date)]

        # Add the weekly consent summary details if errors exist for newly authored consents
        if self._has_needs_correcting(weekly_errors):
            # Add section description text
            self.row_pos += 1
            section_text_cell = self._make_a1_notation(self.row_pos)
            self._add_report_rows(section_text_cell,
                                [['Outstanding issues for consents authored between {} and {} (by HPO/Organization)'\
                                  .format(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))]]
            )
            self._add_report_formatting(section_text_cell, self.format_specs.get('bold_text'))
            self._add_errors_by_org(df=weekly_errors)
        else:
            text_cell = self._make_a1_notation(self.row_pos)
            text_str = 'No outstanding issues for recent consents authored between {} and {}'.format(
                start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            )
            self._add_report_rows(text_cell, [[text_str]])
            self._add_report_formatting(text_cell, self.format_specs.get('italic_text'))
            self.row_pos += 1

        self.row_pos += 1


    def create_weekly_report(self, spreadsheet):
        existing_sheets = spreadsheet.worksheets()
        # Perform rolling deletion of the oldest reports so we keep a pre-defined maximum number of daily reports
        # NOTE:  this assumes all the reports in the file were generated in order, with the most recent date at the
        # leftmost tab (index 0).   This deletes sheets from the existing_sheets list, starting at the rightmost tab
        for ws_index in range(len(existing_sheets), self.max_weekly_reports - 1, -1):
            spreadsheet.del_worksheet(existing_sheets[ws_index - 1])

        # Add the new worksheet (to leftmost tab position / index 0)
        tab_title = f'{self.start_date.strftime("%Y-%m-%d")} to {self.end_date.strftime("%Y-%m-%d")}'
        self.worksheet = spreadsheet.add_worksheet(tab_title,
                                                   rows=self.sheet_rows,
                                                   cols=self.sheet_cols,
                                                   index=1)

        # Add Report title text indicating date range covered
        report_title_str = 'Consent Validation Status Report for {} to {}'.format(
            self.start_date.strftime("%b %-d %Y"),
            self.end_date.strftime("%b %-d %Y")
        )
        title_cell = self._make_a1_notation(self.row_pos)
        self._add_report_rows(title_cell, [[report_title_str]])
        self._add_report_formatting(title_cell, self.format_specs.get('bold_text'))
        self._add_text_rows(
            text_rows=[['Notes:'],
                       ['Participant and consent counts currently limited to Vibrent participants only'],
                       ['Participants Not Yet Validated count may fluctuate due to newly consented participants ' +\
                        'whose consent files are pending validation'],
                       ['File resolutions include retransmission of files which are successfully validated, ' +\
                        'or correction of any false positive issue notifications from automated validation tools']],
            format_spec=self.format_specs.get('legend_text'),
            row_pos=self.row_pos+1)

        self.row_pos += 2

        #-- Generate main content of report  --
        # Validation burndown: show how many participants have had their consent files validated, # with issues, etc.
        # File issue burndown:  show how many outstanding file issues have been resolved (cumulative and in past week)
        # Aggregate outstanding counts:  Breakdown of outstanding issues by consent type and participants impacted
        # Recent errors:  Newly detected validation errors from recently authored consents (authored in past week)
        self.add_weekly_validation_burndown_section()
        self.add_weekly_file_issue_burndown_section()
        self.add_weekly_aggregate_outstanding_counts_section()
        self.add_weekly_recent_errors_section()
        # Inject whitespace
        self.row_pos += 2

        # Breakdown of all outstanding issues by HPO/Organization (if any issues still exist)
        if self._has_needs_correcting(self.consent_df):
            self._add_text_rows(
                text_rows=[['All Outstanding Issues including Retrospective Validations (by HPO/Organization)']],
                format_spec=self.format_specs.get('bold_text'))
            # Add the HPO/Organization breakdown of outstanding issues
            self._add_errors_by_org()

        self._write_report_content()

    def execute(self):
        """
        Execute the WeeklyConsentReport builder
        """

        _logger.info('Setting up database connection and google doc access...')
        self._connect_to_rdr_replica()
        service_key_info = gcp_get_iam_service_key_info(self.gcp_env.service_key_id)
        gs_creds = gspread.service_account(service_key_info['key_path'])
        gs_file = gs_creds.open_by_key(self.doc_id)

        _logger.info('Retrieving consent validation records...')
        # consent_df will contain all the outstanding NEEDS_CORRECTING issues that still need resolution
        self.consent_df = self._get_consent_validation_dataframe(
            self.report_sql.format_map(SafeDict(end_date=self.end_date.strftime("%Y-%m-%d"),
                                                start_date=self.start_date.strftime("%Y-%m-%d"),
                                                report_date=self.report_date.strftime("%Y-%m-%d"))))
        # Workaround:  filtering out results for older consents where programmatic PDF validation flagged files where it
        # couldn't find signature/signing date, even though the files looked okay on visual inspection
        self.consent_df = self.remove_potential_false_positives_for_needs_correcting(self.consent_df)

        # Get all the resolved/OBSOLETE issues for generating resolution stats
        self.resolved_df = self.get_resolved_consent_issues_dataframe()
        _logger.info('Generating report data...')
        self.create_weekly_report(gs_file)

        _logger.info('Report complete')

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
    parser.add_argument("--doc-id", type=str,
                        help="A google doc ID which can override a [DAILY|WEEKLY]_CONSENT_DOC_ID env var")
    parser.add_argument("--report-type", type=str, default="daily_uploads", metavar='REPORT',
                        help="Report to generate.  Default is daily_uploads")
    parser.add_argument("--report-date", type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help="Date of the consents (authored) in YYYY-MM-DD format.  Default is yesterday's date")
    parser.add_argument("--start-date", type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help="Start date of range for consents (authored) in YYYY-MM-DD format.  Default is 8 days ago")
    parser.add_argument("--end-date", type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help="End date of range for consents (authored) in YYYY-MM-DD format.  Default is 1 day ago")
    parser.add_argument("--csv-file", type=str,
                        help="output filename for the CSV error list. " +\
                                           " Default is YYYYMMDD_consent_errors.csv where YYYYMMDD is the report date")
    parser.add_argument("--sheet-only", default=False, action="store_true",
                        help="Only generate the googlesheet report, skip generating the CSV file")
    parser.add_argument("--csv-only", default=False, action="store_true",
                        help="Only generate the CSV errors file, skip generating google sheet content")
    parser.epilog = f'Possible REPORT types: {{{",".join(REPORT_TYPES)}}}.'
    args = parser.parse_args()


    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        if args.report_type == 'daily_uploads':
            process = DailyConsentReport(args, gcp_env)
        elif args.report_type == 'weekly_status':
            process = WeeklyConsentReport(args, gcp_env)
        else:
            raise("Invalid report type specified")

        exit_code = process.execute()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
